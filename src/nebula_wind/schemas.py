"""Input schema and required columns.

This module defines the exact fields we expect to see in the raw CSV files. The
schema is intentionally explicit because Spark will otherwise infer types from
sample rows and can silently misread malformed sensor data.
"""

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# These are the raw source columns from the telemetry export. The timestamps are
# kept as strings initially so they can be cast and validated consistently.
RAW_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("turbine_id", StringType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("power_output", DoubleType(), True),
    ]
)

# The pipeline relies on a set of required fields for validation and downstream
# transforms. Keeping the set derived from the schema prevents drift between the
# documented contract and actual code.
REQUIRED_COLUMNS = set(RAW_SCHEMA.fieldNames())
