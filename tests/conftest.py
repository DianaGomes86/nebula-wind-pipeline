"""Shared test fixtures for the Nebula Wind pipeline.

This module centralises the Spark test session so each test can work with the
same local Spark configuration. Keeping the fixture here avoids repetition and
ensures the same runtime settings apply to all regression checks.
"""

import pytest
from pyspark.sql import SparkSession


# A single local Spark session is reused across the file so tests do not re-init
# the JVM for each case. This keeps the suite faster while still giving each test
# a clean and isolated DataFrame context.
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("nebula-wind-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
