"""Configuration objects for the pipeline.

The pipeline has a small set of runtime parameters such as file paths,
aggregation window size, and output storage format. Centralising them in a
config object makes the ETL easier to reason about and prevents invalid
settings from silently propagating through the job.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable runtime settings for a single pipeline execution."""

    # These paths are required because the job reads raw input data and writes
    # cleaned data products to distinct output tables.
    input_path: str
    output_path: str

    # Summary windows and anomaly detection thresholds define how much history is
    # aggregated and how aggressive the outlier checks should be.
    window_hours: int = 24
    z_score_threshold: float = 2.0
    timezone: str = "UTC"
    write_format: str = "delta"

    def __post_init__(self) -> None:
        """Reject invalid runtime settings early.

        This is valuable during local development and deployment because the
        pipeline fails fast before reading large datasets or writing partial
        outputs with nonsensical config values.
        """
        if self.window_hours <= 0:
            raise ValueError("window_hours must be positive")
        if self.z_score_threshold <= 0:
            raise ValueError("z_score_threshold must be positive")
        if self.write_format not in {"delta", "parquet"}:
            raise ValueError("write_format must be 'delta' or 'parquet'")

