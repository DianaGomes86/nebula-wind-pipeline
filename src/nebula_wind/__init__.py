"""Nebula Wind telemetry pipeline.

This package contains the ETL and anomaly-detection logic used to ingest wind
sensor data, clean and aggregate it, and surface turbine-level anomalies.
The module exposes a version marker so the package can be tracked consistently
across local development and deployment environments.
"""

# Keep the package version in one place so release tooling and runtime checks
# can reference a stable identifier without needing to inspect source files.
__version__ = "0.1.0"

