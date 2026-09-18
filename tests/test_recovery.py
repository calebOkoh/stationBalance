"""The synthetic recovery test -- README section 6, "Recovery test (3.2 / 3.3)".

The ledger diagnostics in 3.10 check internal CONSISTENCY. They do not check
that the event-emission rules are CORRECT. This does, and without external
data: take trips with known station pairs and timings, inject synthetic van
moves, run the real 3.2/3.3 functions, and assert exact recovery of every
injected move.

This is a GATE, not a nice-to-have. Step 3.1 is the one stage where a mistake
produces plausible-looking but wrong output that no downstream check obviously
catches (README section 7, risk #2), and step 6.6 -- which would have validated
the thresholds against real data -- is deferred past delivery.

It imports the ACTUAL functions from pipelines/labels/job.py rather than
reimplementing them, so it cannot pass against a copy that has drifted.

Run:  python3 tests/test_recovery.py          (skips cleanly without pyspark)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
sys.path.insert(0, str(ROOT / "pipelines" / "labels"))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError:
    print("SKIP: pyspark is not installed here.")
    print("      This gate MUST run before trusting phase 3 output. On the "
          "cluster:")
    print("      spark-submit tests/test_recovery.py")
    raise SystemExit(0)

import job as labels_job  # noqa: E402

CFG = {
    "labels": {
        "rebalance_short_gap_hours": 6,
        "rebalance_long_gap_hours": 72,
        "fleet_exit_days": 30,
        "edge_offset_hours": 1,
    }
}

T0 = datetime(2026, 3, 2, 8, 0)  # a Monday, well clear of a DST boundary


def _trip(trip_id, bike, start, dur_min, a, b):
    return (trip_id, bike, start, start + timedelta(minutes=dur_min), a, b)


def build(spark, rows):
    df = spark.createDataFrame(
        rows,
        "trip_id long, bike_id string, start_time timestamp, end_time timestamp, "
        "start_station_id int, end_station_id int",
    )
    return labels_job.bike_trajectories(df)


def test_no_event_when_bike_stays_put(spark):
    """A bike whose next trip starts where the last one ended was not moved."""
    traj = build(spark, [
        _trip(1, "B1", T0, 10, 100, 200),
        _trip(2, "B1", T0 + timedelta(hours=3), 10, 200, 300),
    ])
    events = labels_job.rebalancing_events(traj, CFG).collect()
    assert len(events) == 0, f"expected no rebalance, got {events}"


def test_short_gap_emits_pair_at_midpoint(spark):
    """g < 6h: both events at the gap midpoint."""
    end = T0 + timedelta(minutes=10)
    nxt = end + timedelta(hours=2)
    traj = build(spark, [
        _trip(1, "B1", T0, 10, 100, 200),
        _trip(2, "B1", nxt, 10, 900, 300),
    ])
    events = {(r["station_id"], r["event_type"], r["event_time"])
              for r in labels_job.rebalancing_events(traj, CFG).collect()}

    midpoint = end + timedelta(hours=1)
    assert (200, "reb_out", midpoint) in events, events
    assert (900, "reb_in", midpoint) in events, events
    assert len(events) == 2


def test_medium_gap_splits_to_edges(spark):
    """6h <= g < 72h: reb_out at end+1h, reb_in at next_start-1h.

    The priors are asymmetric -- removed shortly after the last trip, placed
    shortly before the next -- so a single midpoint would put the van move at a
    time neither event happened.
    """
    end = T0 + timedelta(minutes=10)
    nxt = end + timedelta(hours=24)
    traj = build(spark, [
        _trip(1, "B1", T0, 10, 100, 200),
        _trip(2, "B1", nxt, 10, 900, 300),
    ])
    events = {(r["station_id"], r["event_type"], r["event_time"])
              for r in labels_job.rebalancing_events(traj, CFG).collect()}

    assert (200, "reb_out", end + timedelta(hours=1)) in events, events
    assert (900, "reb_in", nxt - timedelta(hours=1)) in events, events


def test_long_gap_tagged_maintenance(spark):
    """g >= 72h: same edge split, tagged maintenance_removal."""
    end = T0 + timedelta(minutes=10)
    nxt = end + timedelta(hours=100)
    traj = build(spark, [
        _trip(1, "B1", T0, 10, 100, 200),
        _trip(2, "B1", nxt, 10, 900, 300),
    ])
    kinds = {r["event_kind"] for r in labels_job.rebalancing_events(traj, CFG).collect()}
    assert kinds == {"maintenance_removal"}, kinds


def test_exact_recovery_of_injected_moves(spark):
    """The headline assertion: every injected van move is recovered, exactly.

    Ten bikes, each given one known move at a known time between known
    stations. Recovery must be exact -- not approximate, not mostly.
    """
    rows, expected = [], set()
    trip_id = 0

    for i in range(10):
        bike = f"B{i}"
        a, b, c = 100 + i, 200 + i, 300 + i
        start = T0 + timedelta(days=i)
        end = start + timedelta(minutes=15)
        nxt = end + timedelta(hours=24)  # medium gap -> edge split

        trip_id += 1
        rows.append(_trip(trip_id, bike, start, 15, a, b))
        trip_id += 1
        rows.append(_trip(trip_id, bike, nxt, 15, c, a))

        expected.add((b, "reb_out", end + timedelta(hours=1)))
        expected.add((c, "reb_in", nxt - timedelta(hours=1)))

    traj = build(spark, rows)
    got = {(r["station_id"], r["event_type"], r["event_time"])
           for r in labels_job.rebalancing_events(traj, CFG).collect()}

    assert got == expected, (
        f"recovery is not exact\n  missing: {expected - got}\n  spurious: {got - expected}"
    )


def test_quarter_boundary_injects_no_false_rebalance(spark):
    """The failure mode step 3.1 exists to prevent.

    Two trips by one bike either side of a quarter boundary, ending and
    starting at the SAME station. A per-quarter pass would see two separate
    bike histories and invent a rebalance. A single global pass sees one bike
    that did not move.
    """
    traj = build(spark, [
        _trip(1, "B1", datetime(2026, 3, 31, 22, 0), 20, 100, 200),
        _trip(2, "B1", datetime(2026, 4, 1, 9, 0), 20, 200, 300),
    ])
    events = labels_job.rebalancing_events(traj, CFG).collect()
    assert len(events) == 0, f"false rebalance across the quarter boundary: {events}"


def test_fleet_entry_and_exit(spark):
    """3.3 -- without exit events a retired bike inflates its last station
    permanently; without entry events new bikes appear from nowhere."""
    window_end = T0 + timedelta(days=365)
    traj = build(spark, [
        _trip(1, "B1", T0, 10, 100, 200),
        _trip(2, "B1", T0 + timedelta(hours=2), 10, 200, 300),
    ])
    events = {(r["station_id"], r["event_type"], r["event_kind"])
              for r in labels_job.fleet_events(traj, CFG, window_end).collect()}

    assert (100, "reb_in", "fleet_entry") in events, events
    assert (300, "reb_out", "fleet_exit") in events, events


def test_recent_last_trip_is_not_retired(spark):
    """A bike whose last trip is inside the 30-day threshold is idle, not
    retired. Emitting an exit would remove a bike still in service."""
    window_end = T0 + timedelta(days=5)
    traj = build(spark, [_trip(1, "B1", T0, 10, 100, 200)])
    kinds = {r["event_kind"]
             for r in labels_job.fleet_events(traj, CFG, window_end).collect()}
    assert "fleet_exit" not in kinds, kinds


def main() -> int:
    # local[2] when run from a checkout that has Spark; whatever the cluster
    # set when submitted through scripts/25_run_gate.sh. Pinning local[2]
    # unconditionally would force the whole gate into the driver container on
    # EMR Serverless, which is exactly the environment the gate exists to
    # validate against.
    builder = (SparkSession.builder
               .appName("station-balance/recovery-test")
               .config("spark.sql.session.timeZone", "America/New_York")
               .config("spark.sql.shuffle.partitions", "4"))
    if not SparkSession.getActiveSession() and "SPARK_APPLICATION_ID" not in os.environ:
        builder = builder.master(os.environ.get("RECOVERY_TEST_MASTER", "local[2]"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn(spark)
                print(f"PASS {name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"FAIL {name}: {exc}")

    spark.stop()
    if failures:
        print(f"\n{failures} failure(s). Phase 3 output is NOT trustworthy "
              "until these pass (README section 6).")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
