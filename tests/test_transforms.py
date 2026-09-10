"""Regression tests for the telemetry transformation pipeline.

These tests cover the main data-quality rules: standardising timestamps,
cleaning invalid values, aggregating summaries, and detecting anomalous
machines. They protect the core business logic from accidental regressions when
new rules are added.
"""

from datetime import datetime, timezone

from nebula_wind.transforms import (
    calculate_summary,
    clean_measurements,
    identify_anomalous_turbines,
    standardise,
)


# This test ensures the standardisation step rejects unusable timestamps and
# collapses duplicate readings to a single valid record. This protects the
# pipeline from double-counting telemetry during reruns or retransmissions.
def test_standardise_drops_bad_keys_and_duplicates(spark):
    rows = [
        ("2026-01-01T01:00:00Z", "T01", 10.0, 180.0, 2.0),
        ("2026-01-01T01:00:00Z", "T01", 10.0, 180.0, 2.0),
        (None, "T02", 10.0, 180.0, 2.0),
    ]
    df = spark.createDataFrame(
        rows, ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output_mw"]
    )
    assert standardise(df).count() == 1


# Wind data often contains invalid or missing values. This test checks that the
# cleaning rule imputes reasonable wind readings while still rejecting power
# values that would corrupt the operational signal.
def test_cleaning_imputes_wind_but_rejects_invalid_power(spark):
    rows = [
        (datetime(2026, 1, 1, 0, tzinfo=timezone.utc), "T01", 10.0, 180.0, 2.0),
        (datetime(2026, 1, 1, 1, tzinfo=timezone.utc), "T01", None, 400.0, 3.0),
        (datetime(2026, 1, 1, 2, tzinfo=timezone.utc), "T01", 12.0, 190.0, -1.0),
    ]
    df = spark.createDataFrame(
        rows, ["event_ts", "turbine_id", "wind_speed", "wind_direction", "power_output_mw"]
    )
    result = clean_measurements(df).orderBy("event_ts").collect()
    assert len(result) == 2
    assert result[1].wind_speed == 10.0
    assert result[1].wind_direction == 180.0


# Summary aggregation should collapse a turbine's measurements into consistent
# window-level statistics such as min, max, average, and count.
def test_summary_statistics(spark):
    rows = [
        (datetime(2026, 1, 1, 2, tzinfo=timezone.utc), "T01", 10.0, 180.0, 1.0),
        (datetime(2026, 1, 1, 3, tzinfo=timezone.utc), "T01", 11.0, 190.0, 3.0),
    ]
    df = spark.createDataFrame(
        rows, ["event_ts", "turbine_id", "wind_speed", "wind_direction", "power_output_mw"]
    )
    row = calculate_summary(df).first()
    assert (row.min_power_mw, row.max_power_mw, row.avg_power_mw, row.reading_count) == (
        1.0,
        3.0,
        2.0,
        2,
    )


# This validates the fleet anomaly rule: a turbine far below the group average
# should be flagged, while peers with normal output remain unflagged.
def test_fleet_anomaly_is_flagged(spark):
    rows = []
    for number in range(1, 11):
        rows.append(
            (
                f"T{number:02d}",
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                5.0,
            )
        )
    rows.append(
        (
            "T11",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            0.0,
        )
    )
    summary = spark.createDataFrame(
        rows, ["turbine_id", "period_start", "period_end", "avg_power_mw"]
    )
    flagged = identify_anomalous_turbines(summary).filter("is_anomaly").collect()
    assert [row.turbine_id for row in flagged] == ["T11"]
