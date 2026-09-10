"""Generate deterministic sample CSVs with gaps, nulls and outliers.

This utility is intended for local development and tests. It produces a realistic
wind telemetry dataset with missing rows, nulls, and one intentionally weak
sensor so the cleaning and anomaly-detection logic can be exercised without
needing production data.
"""

import argparse
import csv
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate(output: Path, days: int = 30, seed: int = 42) -> None:
    """Create a synthetic data set for the pipeline.

    The generated records simulate a fleet of turbines across multiple groups,
    with periodic gaps, random nulls, and a persistent underperformer. This is
    useful both for manual exploration and automated regression tests.
    """
    random.seed(seed)
    output.mkdir(parents=True, exist_ok=True)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    headers = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    handles = []
    try:
        writers = {}
        # Split the synthetic data by group to reflect how raw files may arrive in
        # multiple shards or partitions.
        for group in range(1, 4):
            handle = (output / f"data_group_{group}.csv").open("w", newline="", encoding="utf-8")
            handles.append(handle)
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            writers[group] = writer

        for hour in range(days * 24):
            event_ts = start + timedelta(hours=hour)
            for turbine_number in range(1, 16):
                # Introduce missing rows to mimic sensor outages or delayed uploads.
                if random.random() < 0.025:
                    continue
                group = (turbine_number - 1) // 5 + 1
                wind_speed = max(0.0, random.gauss(11.0, 3.0))
                # Smooth daily direction cycle. Always use the standard-library value of pi.
                direction = (180 + 80 * math.sin(2 * math.pi * hour / 24)) % 360
                power = min(8.0, 0.004 * wind_speed**3) + random.gauss(0, 0.12)
                if turbine_number == 15:
                    # A deliberately weak turbine creates an anomaly signal that the
                    # fleet scoring logic should identify.
                    power *= 0.30
                writers[group].writerow(
                    {
                        "timestamp": event_ts.isoformat(),
                        "turbine_id": f"T{turbine_number:02d}",
                        "wind_speed": "" if random.random() < 0.01 else round(wind_speed, 3),
                        "wind_direction": round(direction, 3),
                        "power_output": round(max(0.0, power), 3),
                    }
                )
    finally:
        for handle in handles:
            handle.close()


def main() -> None:
    """CLI entry point for generating a fresh sample dataset."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/raw")
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    generate(Path(args.output), args.days)


if __name__ == "__main__":
    main()
