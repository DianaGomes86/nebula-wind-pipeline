"""Pipeline orchestration and storage.

This file acts as the orchestration layer: it reads raw telemetry, applies the
transformation pipeline, and writes the cleaned outputs in a structured layout.
Each table is written to a dedicated path so downstream consumers can query the
latest cleaned, aggregated, and anomalous views independently.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nebula_wind.config import PipelineConfig
from nebula_wind.schemas import RAW_SCHEMA
from nebula_wind.transforms import (
    calculate_summary,
    clean_measurements,
    identify_anomalous_turbines,
    standardise,
)


def read_raw(spark: SparkSession, input_path: str) -> DataFrame:
    """Load the raw CSV dataset using the known telemetry schema.

    The dataset is expected to arrive as a CSV with a header row and a set of
    telemetry fields. We also attach metadata such as ingested_at and the source
    file so audits can trace an individual record back to its origin.
    """
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(input_path)
        .withColumnRenamed("power_output", "power_output_mw")
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )


def write_table(df: DataFrame, path: str, write_format: str, partitions: list[str]) -> None:
    """Persist a DataFrame to disk using overwrite semantics and optional partitioning."""
    writer = df.write.format(write_format).mode("overwrite").option("overwriteSchema", "true")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(path)


def run(spark: SparkSession, config: PipelineConfig) -> dict[str, DataFrame]:
    """Execute the full data processing flow for a single job.

    The sequence is intentionally simple: configure Spark, read input, clean,
    aggregate, detect anomalies, and then persist the derived tables to disk.
    """
    spark.conf.set("spark.sql.session.timeZone", config.timezone)

    # Load the raw sensor feed and enrich it with provenance metadata.
    raw = read_raw(spark, config.input_path)

    # First, standardise the timestamps and strip duplicate or malformed records.
    cleaned = clean_measurements(standardise(raw)).withColumn(
        "event_date", F.to_date("event_ts")
    )

    # Summaries group the cleaned measurements into operational windows so that
    # engineers can track turbine performance over each period.
    summary = calculate_summary(cleaned, config.window_hours).withColumn(
        "period_date", F.to_date("period_start")
    )

    # Fleet-level z-score detection flags turbines whose average output deviates
    # materially from the group during the same period.
    anomalies = identify_anomalous_turbines(summary, config.z_score_threshold)

    base = Path(config.output_path)

    # Write each stage to a dedicated table so downstream jobs can consume the
    # exact artifact they need without recomputing the pipeline from scratch.
    write_table(cleaned, str(base / "cleaned_measurements"), config.write_format, ["event_date"])
    write_table(summary, str(base / "turbine_summary"), config.write_format, ["period_date"])
    write_table(anomalies, str(base / "turbine_anomalies"), config.write_format, ["period_date"])
    return {"cleaned": cleaned, "summary": summary, "anomalies": anomalies}
