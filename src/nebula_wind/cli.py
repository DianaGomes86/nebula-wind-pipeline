"""Command-line entry point.

This module exposes the operational interface for the project. It parses the
user's command-line arguments, builds a Spark session with the required format
configuration, and invokes the ETL pipeline. The script is intentionally
minimal so that ad hoc runs and production jobs use the same code path.
"""

import argparse

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from nebula_wind.config import PipelineConfig
from nebula_wind.pipeline import run


def build_spark(write_format: str) -> SparkSession:
    """Create the Spark session and enable Delta support when requested.

    Delta output requires extra SQL extensions and catalog registration. For
    parquet output, we keep the default Spark setup because the rest of the
    pipeline is format-agnostic.
    """
    builder = SparkSession.builder.appName("NebulaWind")
    if write_format == "delta":
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    return builder.getOrCreate()


def parse_args() -> argparse.Namespace:
    """Define the CLI contract for pipeline execution.

    These arguments control the input dataset, the output location, the time
    window used for summaries, and whether the pipeline writes Delta or Parquet
    tables.
    """
    parser = argparse.ArgumentParser(description="Process wind turbine telemetry")
    parser.add_argument("--input", required=True, help="CSV file or glob")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--window-hours", type=int, default=24)
    parser.add_argument("--threshold", type=float, default=2.0)
    parser.add_argument("--format", choices=("delta", "parquet"), default="delta")
    return parser.parse_args()


def main() -> None:
    """Run the pipeline from the command line and show suspicious turbines."""
    args = parse_args()
    config = PipelineConfig(
        input_path=args.input,
        output_path=args.output,
        window_hours=args.window_hours,
        z_score_threshold=args.threshold,
        write_format=args.format,
    )
    # Construct the active Spark session once and close it in a finally block so
    # the process cannot leak worker JVMs on failure.
    spark = build_spark(config.write_format)
    try:
        outputs = run(spark, config)
        # Display the anomaly table so operators can immediately see which
        # turbines crossed the configured threshold during the latest run.
        outputs["anomalies"].filter("is_anomaly").orderBy("period_start", "turbine_id").show(
            truncate=False
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

