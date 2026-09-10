# Nebula Wind Pipeline

A compact PySpark project for validating wind-turbine telemetry, calculating rolling summaries, detecting outliers, and writing clean outputs for downstream analysis.

## What it does

The pipeline reads raw telemetry CSVs, validates the schema, removes duplicate or broken records, cleans invalid sensor values, calculates 24-hour turbine summaries, and flags abnormal turbines by comparing each turbine to the fleet in the same period.

```text
CSV input -> schema validation -> deduplication -> cleaning
-> 24h turbine summaries -> fleet z-score anomaly detection -> output tables
```

## Input contract

Each CSV should include a header with these columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `timestamp` | ISO-8601 string | Measurement time in UTC |
| `turbine_id` | string | Unique turbine identifier |
| `wind_speed` | double | Wind speed in m/s |
| `wind_direction` | double | Direction in degrees, in the range [0, 360) |
| `power_output` | double | Generated power in MW |

The raw field `power_output` is renamed internally to `power_output_mw`.

## Data-quality rules

- Invalid timestamps and blank turbine IDs are rejected.
- Duplicate `(turbine_id, event_ts)` records are removed to keep reruns idempotent.
- Wind speed outside `0..75` and direction outside `0..360` are treated as missing.
- Missing or negative power values are rejected instead of imputed.
- The pipeline uses a 24-hour tumbling window by default.
- An anomaly is flagged when a turbine's 24-hour average differs from the fleet by more than 2 standard deviations.

## Local setup

Requirements: Java 11 or 17 and Python 3.10+.

```bash
python -m venv .venv
source .venv/bin/activate

# Install the project dependencies from the requirements file
pip install -r requirements.txt

# Or install the package in editable mode if needed for local development
pip install -e '.[dev]'
```

If you are setting up the project for the first time, the `requirements.txt` install is the simplest option. The editable install is useful when you want the package to reflect local source changes immediately.

### Generate sample data

```bash
nebula-generate --output data/raw --days 30
```

### Run the pipeline

```bash
# Default output format is Delta
nebula-wind --input 'data/raw/*.csv' --output data/processed

# Optional fallback without Delta dependencies
nebula-wind --input 'data/raw/*.csv' --output data/processed --format parquet
```

### Validate

```bash
pytest -q
ruff check .
```

## Output tables

The job writes three derived tables under the configured output directory:

- `cleaned_measurements`: validated telemetry, partitioned by event date
- `turbine_summary`: min, max, average, stddev, and count per turbine per period
- `turbine_anomalies`: summary rows with fleet baseline and anomaly flag

## Notes

The project is intentionally built around DataFrame-based transformations so the business logic stays separate from I/O and is easy to test.

This is a proof of concept, but the next production step would be to add better turbine-model baselines, object storage, incremental writes, orchestration, and quality-alerting around missing data and duplicate keys.
