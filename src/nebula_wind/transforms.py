"""Pure DataFrame transformations used by the pipeline.

The functions in this module are intentionally side-effect free: they accept a
Spark DataFrame and return a transformed DataFrame. This makes them easy to test
in isolation and keeps the business rules separate from the file I/O layer.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def validate_columns(df: DataFrame) -> None:
    """Ensure the input frame includes the fields required for downstream logic."""
    required = {"timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output_mw"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Input is missing required columns: {sorted(missing)}")


def standardise(df: DataFrame) -> DataFrame:
    """Parse types, reject unusable rows, and remove duplicate measurements."""
    validate_columns(df)
    parsed = (
        # Convert the source timestamp string into a proper timestamp so it can be
        # used in time-based filters and windows.
        df.withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("turbine_id", F.trim("turbine_id"))
        .drop("timestamp")
        # Ignore invalid timestamps or empty turbine identifiers before further
        # processing because they cannot form a reliable telemetry record.
        .filter(F.col("event_ts").isNotNull() & (F.length("turbine_id") > 0))
    )
    # A retransmitted reading has the same natural key. Keeping one makes reruns
    # idempotent and avoids double-counting the same event.
    return parsed.dropDuplicates(["turbine_id", "event_ts"])


def clean_measurements(df: DataFrame) -> DataFrame:
    """Apply physical bounds and median-impute nullable numeric measurements.

    Invalid power is not imputed: manufacturing power would bias both operational
    reporting and anomaly detection. Invalid wind measurements are imputed by the
    turbine's median so a valid power reading remains usable.
    """
    # First, enforce realistic physical bounds. Values outside these ranges are
    # treated as invalid and removed from the dataset rather than silently
    # altering the underlying signal.
    physically_valid = (
        df.withColumn(
            "wind_speed",
            F.when((F.col("wind_speed") >= 0) & (F.col("wind_speed") <= 75), F.col("wind_speed")),
        )
        .withColumn(
            "wind_direction",
            F.when(
                (F.col("wind_direction") >= 0) & (F.col("wind_direction") < 360),
                F.col("wind_direction"),
            ),
        )
        .filter((F.col("power_output_mw") >= 0) & F.col("power_output_mw").isNotNull())
    )

    turbine_window = Window.partitionBy("turbine_id")
    return (
        # Fill missing wind values using the turbine's median reading, which is
        # far less sensitive to occasional spikes than the mean.
        physically_valid.withColumn(
            "wind_speed",
            F.coalesce(
                "wind_speed",
                F.percentile_approx("wind_speed", F.lit(0.5)).over(turbine_window),
            ),
        )
        .withColumn(
            "wind_direction",
            F.coalesce(
                "wind_direction",
                F.percentile_approx("wind_direction", F.lit(0.5)).over(turbine_window),
            ),
        )
        # If either wind metric is still missing after the median fill, the record
        # is not useful for analysis and must be removed.
        .filter(F.col("wind_speed").isNotNull() & F.col("wind_direction").isNotNull())
    )


def add_period(df: DataFrame, window_hours: int) -> DataFrame:
    """Assign deterministic tumbling windows (24 hours by default)."""
    return df.withColumn("period", F.window("event_ts", f"{window_hours} hours"))


def calculate_summary(df: DataFrame, window_hours: int = 24) -> DataFrame:
    """Aggregate each turbine into summary statistics for each time window."""
    return (
        add_period(df, window_hours)
        .groupBy("turbine_id", "period")
        .agg(
            F.min("power_output_mw").alias("min_power_mw"),
            F.max("power_output_mw").alias("max_power_mw"),
            F.avg("power_output_mw").alias("avg_power_mw"),
            F.stddev_samp("power_output_mw").alias("power_stddev_mw"),
            F.count("*").alias("reading_count"),
        )
        .select(
            "turbine_id",
            F.col("period.start").alias("period_start"),
            F.col("period.end").alias("period_end"),
            "min_power_mw",
            "max_power_mw",
            "avg_power_mw",
            "power_stddev_mw",
            "reading_count",
        )
    )


def identify_anomalous_turbines(summary: DataFrame, threshold: float = 2.0) -> DataFrame:
    """Flag turbines whose average output deviates materially from the fleet."""
    # Compute the fleet-level average and standard deviation within each period so
    # we can compare each turbine against the expected operating range.
    fleet_period = Window.partitionBy("period_start", "period_end")
    scored = (
        summary.withColumn("fleet_avg_power_mw", F.avg("avg_power_mw").over(fleet_period))
        .withColumn("fleet_stddev_mw", F.stddev_samp("avg_power_mw").over(fleet_period))
        .withColumn(
            "z_score",
            F.when(
                F.col("fleet_stddev_mw") > 0,
                (F.col("avg_power_mw") - F.col("fleet_avg_power_mw"))
                / F.col("fleet_stddev_mw"),
            ).otherwise(F.lit(0.0)),
        )
    )
    # A z-score above the configured threshold marks a period where the turbine is
    # unusually far from its peers and therefore likely anomalous.
    return scored.withColumn("is_anomaly", F.abs("z_score") > F.lit(threshold))

