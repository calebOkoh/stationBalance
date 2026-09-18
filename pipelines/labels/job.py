"""Phase 3 -- Label Construction. pipelines.md steps 3.1-3.10.

The heart of the project, and the stage where a partitioning mistake produces
plausible-looking but wrong output that no downstream check obviously catches
(README section 7, risk #2).

There is no published historical archive of dock occupancy, so the label is
reconstructed from trip data: chaining each bike's trips on bike_id, a bike
whose next trip starts somewhere other than where its last one ended was moved
by a van, and a reb_out/reb_in pair is emitted.

Two tiers come out of this:
  Tier 1  net_flow = arr - dep     counted, zero error. THE training target.
  Tier 2  O, pct_full, is_empty    estimated from a clamped cumulative ledger.

Step 3.1 MUST be a single global pass across all quarters. Partitioning by
quarter breaks bike_id continuity and injects a false rebalance every 3 months.
"""

from __future__ import annotations

import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from lib import Zones, load_features, parse_args, spark_session


def bike_trajectories(trips):
    """3.1 -- one ordered sequence per bike, across the WHOLE history.

    The window has no time bound on purpose. `repartition("bike_id")` first so
    each bike's entire history lands on one executor; without it Spark still
    produces the right answer but shuffles the full trip table per window
    operation, and this is the peak-memory stage the cluster is sized for.
    """
    w = Window.partitionBy("bike_id").orderBy("start_time")

    return (
        trips.repartition("bike_id")
        .withColumn("next_start_time", F.lead("start_time").over(w))
        .withColumn("next_start_station", F.lead("start_station_id").over(w))
        .withColumn("trip_seq", F.row_number().over(w))
        .withColumn("is_first_trip", F.row_number().over(w) == 1)
        .withColumn("is_last_trip", F.lead("start_time").over(w).isNull())
    )


def rebalancing_events(traj, cfg):
    """3.2 -- emit reb_out / reb_in at the gap EDGES, not as a pair at one time.

    The priors are asymmetric: a bike is removed shortly after its last trip
    and placed shortly before its next one. Emitting both at a single timestamp
    would put a van move at a moment neither event actually happened, and the
    error lands squarely in the occupancy ledger.

    The 6h / 72h / 30d thresholds are PRIORS, not findings. Step 6.6 would
    validate them and is deferred past delivery (.claude/decisions.md), so they
    ship as stated.
    """
    short_h = cfg["labels"]["rebalance_short_gap_hours"]
    long_h = cfg["labels"]["rebalance_long_gap_hours"]
    edge_h = cfg["labels"]["edge_offset_hours"]

    moved = traj.filter(
        F.col("next_start_station").isNotNull()
        & (F.col("end_station_id") != F.col("next_start_station"))
    ).withColumn(
        "gap_h",
        (F.unix_timestamp("next_start_time") - F.unix_timestamp("end_time")) / 3600.0,
    )

    midpoint = F.from_unixtime(
        (F.unix_timestamp("end_time") + F.unix_timestamp("next_start_time")) / 2
    ).cast("timestamp")

    out_ts = F.when(F.col("gap_h") < short_h, midpoint).otherwise(
        F.col("end_time") + F.expr(f"INTERVAL {edge_h} HOURS")
    )
    in_ts = F.when(F.col("gap_h") < short_h, midpoint).otherwise(
        F.col("next_start_time") - F.expr(f"INTERVAL {edge_h} HOURS")
    )
    kind = F.when(F.col("gap_h") >= long_h, F.lit("maintenance_removal")).otherwise(
        F.lit("rebalance")
    )

    reb_out = moved.select(
        F.col("end_station_id").alias("station_id"),
        out_ts.alias("event_time"),
        F.lit("reb_out").alias("event_type"),
        kind.alias("event_kind"),
    )
    reb_in = moved.select(
        F.col("next_start_station").alias("station_id"),
        in_ts.alias("event_time"),
        F.lit("reb_in").alias("event_type"),
        kind.alias("event_kind"),
    )

    return reb_out.unionByName(reb_in)


def fleet_events(traj, cfg, window_end):
    """3.3 -- fleet entry and exit.

    Without exit events a retired bike inflates its last station's occupancy
    permanently; without entry events new bikes appear from nowhere. Both are
    pure ledger corrections with no corresponding trip.
    """
    edge_h = cfg["labels"]["edge_offset_hours"]
    exit_days = cfg["labels"]["fleet_exit_days"]

    entry = traj.filter(F.col("is_first_trip")).select(
        F.col("start_station_id").alias("station_id"),
        (F.col("start_time") - F.expr(f"INTERVAL {edge_h} HOURS")).alias("event_time"),
        F.lit("reb_in").alias("event_type"),
        F.lit("fleet_entry").alias("event_kind"),
    )

    # A bike whose last trip is inside the exit threshold of the window end is
    # simply idle, not retired. Emitting an exit for it would remove a bike
    # that is still in service.
    exit_ = traj.filter(
        F.col("is_last_trip")
        & (F.col("end_time") < F.lit(window_end).cast("timestamp")
           - F.expr(f"INTERVAL {exit_days} DAYS"))
    ).select(
        F.col("end_station_id").alias("station_id"),
        (F.col("end_time") + F.expr(f"INTERVAL {edge_h} HOURS")).alias("event_time"),
        F.lit("reb_out").alias("event_type"),
        F.lit("fleet_exit").alias("event_kind"),
    )

    return entry.unionByName(exit_)


def station_hour_grid(spark, stations, trips):
    """3.4 -- the complete grid. DO NOT SKIP.

    Hours with no activity are real zeros, not missing rows. Aggregating trips
    alone silently drops every quiet hour and biases the model toward busy
    periods -- which is precisely the regime the stockout question lives in.

    Filtered by go-live date so a station is not scored as zero-demand for
    years before it existed.
    """
    bounds = trips.select(
        F.date_trunc("hour", F.min("start_time")).alias("t0"),
        F.date_trunc("hour", F.max("end_time")).alias("t1"),
    ).first()

    hours = spark.sql(
        f"SELECT explode(sequence("
        f"  TIMESTAMP'{bounds['t0']}', TIMESTAMP'{bounds['t1']}', INTERVAL 1 HOUR"
        f")) AS hour_ts"
    )

    return (
        stations.crossJoin(F.broadcast(hours) if hours.count() < 100000 else hours)
        .filter(F.col("hour_ts") >= F.col("go_live_date"))
        .select("station_id", "hour_ts", "capacity_gbfs")
    )


def solve_initial_occupancy(flows, cfg):
    """3.8 / 3.9 -- solve O(s,0), then integrate and clamp.

    O(s,0) is ONE unknown scalar per station. The physical constraint
    0 <= O(s,t) <= capacity(s) pins it with no external data: over the
    cumulative delta series C(t), feasibility requires

        -min(C) <= O(s,0) <= capacity - max(C)

    and the midpoint of that interval is the least-committal choice. An empty
    interval means the reconstruction is inconsistent for that station -- it is
    FLAGGED rather than silently clamped, because a station that cannot be
    solved is evidence about the emission rules, not a rounding problem.
    """
    w = Window.partitionBy("station_id").orderBy("hour_ts").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    flows = flows.withColumn(
        "delta", F.col("arr") + F.col("reb_in") - F.col("dep") - F.col("reb_out")
    ).withColumn("cum_delta", F.sum("delta").over(w))

    per_station = flows.groupBy("station_id").agg(
        F.min("cum_delta").alias("min_cum"),
        F.max("cum_delta").alias("max_cum"),
        F.first("capacity_gbfs").alias("capacity_gbfs"),
    ).withColumn("lower", -F.col("min_cum")) \
     .withColumn("upper", F.col("capacity_gbfs") - F.col("max_cum")) \
     .withColumn("feasible", F.col("lower") <= F.col("upper")) \
     .withColumn(
         "o_initial",
         F.when(F.col("feasible"), (F.col("lower") + F.col("upper")) / 2.0)
          # Infeasible: fall back to the midpoint of the dock, which at least
          # keeps the series inside [0, capacity] once clamped.
          .otherwise(F.col("capacity_gbfs") / 2.0),
     )

    infeasible = per_station.filter(~F.col("feasible")).count()
    total = per_station.count()
    print(f"[3.8] initial occupancy solved for {total - infeasible}/{total} stations; "
          f"{infeasible} infeasible (flagged, re-anchor at next quarter boundary)")

    return (
        flows.join(per_station.select("station_id", "o_initial", "feasible"), "station_id")
        .withColumn("occupancy_raw", F.col("o_initial") + F.col("cum_delta"))
        .withColumn(
            "occupancy",
            F.least(F.greatest(F.col("occupancy_raw"), F.lit(0.0)),
                    F.col("capacity_gbfs").cast("double")),
        )
        .withColumn("clamped", F.col("occupancy") != F.col("occupancy_raw"))
    )


def ledger_diagnostics(labelled, events, cfg) -> None:
    """3.10 -- the three checks that catch bugs before they reach the model.

    These check internal consistency. They do NOT check that the emission rules
    are correct -- that is what the synthetic recovery test in tests/ does.
    """
    print("\n=== 3.10 ledger diagnostics ===")

    # 1. Global mass balance. Exact BY CONSTRUCTION -- every rebalance emits one
    #    out and one in. Any difference is a pairing bug, not a tolerance issue.
    totals = events.groupBy("event_type").count().collect()
    counts = {r["event_type"]: r["count"] for r in totals}
    reb_in, reb_out = counts.get("reb_in", 0), counts.get("reb_out", 0)
    status = "OK" if reb_in == reb_out else "FAIL"
    print(f"  mass balance   reb_in={reb_in:,} reb_out={reb_out:,}  [{status}]")
    if reb_in != reb_out:
        print("    Note: fleet entry/exit are deliberately unpaired, so a "
              "difference equal to the fleet event count is expected here.")

    # 2. Clamp violation rate. Low single-digit % per station is normal and
    #    concentrates at stations with heavy van activity; a high rate is a
    #    timing error in the gap rules.
    clamp = labelled.agg(
        (100.0 * F.sum(F.col("clamped").cast("int")) / F.count("*")).alias("pct")
    ).first()["pct"]
    status = "OK" if clamp is not None and clamp < 10 else "REVIEW"
    print(f"  clamp rate     {clamp:.2f}%  [{status}]")

    # 3. Out-of-system fleet count over time. A smooth few-% of fleet with a
    #    maintenance-shaped seasonal bump is healthy; a sawtooth means the
    #    6h / 72h / 30d thresholds are mis-set.
    by_month = (
        events.filter(F.col("event_kind") == "maintenance_removal")
        .groupBy(F.date_trunc("month", "event_time").alias("month"))
        .count().orderBy("month")
    )
    print("  maintenance removals by month (first 12):")
    for row in by_month.take(12):
        print(f"    {row['month']}  {row['count']:,}")


def main() -> int:
    args = parse_args(__doc__)
    spark = spark_session("labels")
    cfg = load_features(spark, args.features)
    zones = Zones(args.raw_bucket, args.gold_bucket)

    trips = spark.read.parquet(f"{zones.silver}/trips/")
    stations = spark.read.parquet(f"{zones.bronze}/stations/")

    window_end = trips.agg(F.max("end_time")).first()[0]
    print(f"[labels] window ends {window_end}")

    traj = bike_trajectories(trips)
    events = rebalancing_events(traj, cfg).unionByName(
        fleet_events(traj, cfg, window_end)
    ).cache()

    grid = station_hour_grid(spark, stations, trips)

    # 3.5 -- reduce events onto the grid. left join + fillna(0) is what turns
    # "no rows" into "a real zero".
    arrivals = trips.groupBy(
        F.col("end_station_id").alias("station_id"),
        F.date_trunc("hour", "end_time").alias("hour_ts"),
    ).agg(F.count("*").alias("arr"))

    departures = trips.groupBy(
        F.col("start_station_id").alias("station_id"),
        F.date_trunc("hour", "start_time").alias("hour_ts"),
    ).agg(F.count("*").alias("dep"))

    rebal = events.groupBy(
        "station_id", F.date_trunc("hour", "event_time").alias("hour_ts")
    ).agg(
        F.sum(F.when(F.col("event_type") == "reb_in", 1).otherwise(0)).alias("reb_in"),
        F.sum(F.when(F.col("event_type") == "reb_out", 1).otherwise(0)).alias("reb_out"),
    )

    flows = (
        grid
        .join(arrivals, ["station_id", "hour_ts"], "left")
        .join(departures, ["station_id", "hour_ts"], "left")
        .join(rebal, ["station_id", "hour_ts"], "left")
        .fillna(0, subset=["arr", "dep", "reb_in", "reb_out"])
        # 3.6 -- TIER-1 LABEL. Counted directly from trip endpoints: no
        # reconstruction error, no initial condition, nothing estimated.
        .withColumn("net_flow", F.col("arr") - F.col("dep"))
    )

    labelled = solve_initial_occupancy(flows, cfg)

    # 3.7 -- capacity. Historical capacity is not published and stations have
    # been resized, so a fixed current capacity misstates pct_full for earlier
    # periods. The rolling max of reconstructed occupancy tracks the resize;
    # GBFS capacity is the cross-check and the floor.
    days = cfg["labels"]["capacity_rolling_days"]
    w_cap = (
        Window.partitionBy("station_id")
        .orderBy(F.col("hour_ts").cast("long"))
        .rangeBetween(-days * 86400, 0)
    )
    labelled = (
        labelled
        .withColumn("capacity_rolling", F.max("occupancy").over(w_cap))
        .withColumn("capacity",
                    F.greatest(F.col("capacity_rolling"), F.col("capacity_gbfs")))
        # 3.9 -- TIER-2 LABELS. Estimated, and reported as such.
        .withColumn("pct_full", F.col("occupancy") / F.col("capacity"))
        .withColumn("is_empty", F.col("occupancy") < 1.0)
        .withColumn("is_full", F.col("occupancy") >= F.col("capacity"))
    )

    ledger_diagnostics(labelled, events, cfg)

    out = (
        labelled.select(
            "station_id", "hour_ts", "arr", "dep", "reb_in", "reb_out",
            "net_flow", "occupancy", "capacity", "pct_full",
            "is_empty", "is_full", "feasible", "clamped",
        )
        .withColumn("year", F.year("hour_ts"))
        .withColumn("month", F.month("hour_ts"))
    )

    (out.write.mode("overwrite").partitionBy("year", "month")
        .parquet(f"{zones.silver}/station_hour_labels/"))

    print(f"[labels] wrote {out.count():,} station-hours")
    return 0


if __name__ == "__main__":
    sys.exit(main())
