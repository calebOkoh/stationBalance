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
  Tier 2  O, pct_full, is_empty    estimated from the cumulative ledger.

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

    The ordering is TOTAL: start_time alone is not unique -- one bike can have
    two trips stamped at the same minute -- and under a tie lead() picks an
    arbitrary row, so the emitted event set changes between runs of the same
    job on the same data. That was observed: two runs differed by one reb_in
    and the 3.10 mass balance caught it. trip_id breaks every tie, which makes
    the label set reproducible, which is the precondition for comparing two
    models at all.
    """
    w = Window.partitionBy("bike_id").orderBy("start_time", "trip_id")

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


def station_hour_grid(spark, stations, trips, cfg):
    """3.4 -- the complete grid. DO NOT SKIP.

    Hours with no activity are real zeros, not missing rows. Aggregating trips
    alone silently drops every quiet hour and biases the model toward busy
    periods -- which is precisely the regime the stockout question lives in.

    But "no activity this hour" and "this station does not exist yet, or does
    not exist any more" are different things, and only the first is a real zero.
    The grid is therefore bounded at BOTH ends:

      start   max(go_live_date, first hour the station is seen in a trip)
      end     last hour the station is seen -- unless that is within
              fleet_exit_days of the window end, in which case the station is
              simply idle and the grid runs to the end

    The end bound exists because the station table publishes `Status` but no
    RETIREMENT DATE, so a station decommissioned in 2019 still looks eligible
    for every hour from 2022 on. Measured: 13 stations sat at zero for all
    39,440 hours, and the worst 20 held 39.7% of every zero-occupancy hour in
    the dataset (.claude/decisions.md). Those rows carry `is_empty = True` by
    construction and no feature can explain them.

    Activity comes from the trips themselves, so this needs no source the
    project does not have. The idle-vs-retired threshold is the SAME
    `fleet_exit_days` prior step 3.3 uses to decide a bike is retired rather
    than parked -- one concept, one config key, not a second invented number.

    `Virtual Station` is already gone -- phase 2 drops it from the station
    table, not just from the trips.
    """
    exit_days = cfg["labels"]["fleet_exit_days"]

    bounds = trips.select(
        F.date_trunc("hour", F.min("start_time")).alias("t0"),
        F.date_trunc("hour", F.max("end_time")).alias("t1"),
    ).first()

    hours = spark.sql(
        f"SELECT explode(sequence("
        f"  TIMESTAMP'{bounds['t0']}', TIMESTAMP'{bounds['t1']}', INTERVAL 1 HOUR"
        f")) AS hour_ts"
    )

    # A station is "seen" at either end of a trip, so both endpoints count.
    seen = (
        trips.groupBy(F.col("start_station_id").alias("station_id"))
        .agg(F.min("start_time").alias("lo"), F.max("start_time").alias("hi"))
        .unionByName(
            trips.groupBy(F.col("end_station_id").alias("station_id"))
            .agg(F.min("end_time").alias("lo"), F.max("end_time").alias("hi"))
        )
        .groupBy("station_id")
        .agg(F.min("lo").alias("first_seen"), F.max("hi").alias("last_seen"))
    )

    n_stations = stations.count()
    # INNER join: a station with no trips at all in the window is not observed,
    # and an unobserved station is not a zero-demand station -- it is absent.
    observed = stations.join(seen, "station_id", "inner")
    n_observed = observed.count()
    print(f"[3.4] {n_observed}/{n_stations} stations appear in the trip data; "
          f"{n_stations - n_observed} never do and are dropped from the grid")

    window_end = F.lit(bounds["t1"])
    start_bound = F.greatest(
        F.date_trunc("hour", F.col("go_live_date")),
        F.date_trunc("hour", F.col("first_seen")),
    )
    # Same rule as 3.3's fleet exit: a last sighting inside the threshold means
    # idle, not retired, so the grid runs to the end of the window.
    end_bound = F.when(
        F.col("last_seen") >= window_end - F.expr(f"INTERVAL {exit_days} DAYS"),
        window_end,
    ).otherwise(F.date_trunc("hour", F.col("last_seen")))

    return (
        observed.crossJoin(F.broadcast(hours) if hours.count() < 100000 else hours)
        .filter((F.col("hour_ts") >= start_bound) & (F.col("hour_ts") <= end_bound))
        .select("station_id", "hour_ts")
    )


def solve_initial_occupancy(flows, cfg):
    """3.8 / 3.9 -- solve O(s,0), then integrate.

    O(s,0) is ONE unknown scalar per station. The original design pinned it
    with the constraint 0 <= O <= capacity, taking the midpoint of the feasible
    interval -- but that needs a published capacity per station, and the only
    source for one was a live feed. This project reads historical archives
    only, and Indego publishes no historical capacity.

    So the lower constraint alone is used: O(s,0) = -min(cumulative delta),
    the smallest starting occupancy that keeps O(s,t) >= 0 for every hour. That
    is well defined, needs nothing external, and has no free parameter. Its
    cost is that the level is a lower bound rather than a centred estimate --
    the SHAPE of the series, which is what the features predict, is unchanged.

    Capacity is then derived in 3.7 as the rolling maximum of the resulting
    occupancy, which is self-consistent by construction: a station is "full"
    exactly when it reaches the most bikes it has recently held.

    Tier 2 was already shipping without a numeric error bar -- step 6.6 is
    deferred past delivery -- so this trades one unvalidated estimate for
    another that at least has no hidden dependency.
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
    ).withColumn("o_initial", -F.col("min_cum"))

    stats = per_station.agg(
        F.count("*").alias("n"),
        F.avg("o_initial").alias("mean_o0"),
        F.max(F.col("max_cum") - F.col("min_cum")).alias("max_swing"),
    ).first()
    print(f"[3.8] solved O(s,0) for {stats['n']} stations; "
          f"mean {stats['mean_o0']:.1f} bikes, largest swing {stats['max_swing']}")

    return (
        flows.join(per_station.select("station_id", "o_initial"), "station_id")
        # Non-negative by construction: o_initial is exactly -min(cum_delta),
        # so the series touches zero at its minimum and never goes below.
        .withColumn("occupancy", F.col("o_initial") + F.col("cum_delta"))
    )


def ledger_diagnostics(labelled, events, cfg) -> None:
    """3.10 -- the three checks that catch bugs before they reach the model.

    These check internal consistency. They do NOT check that the emission rules
    are correct -- that is what the synthetic recovery test in tests/ does.
    """
    print("\n=== 3.10 ledger diagnostics ===")

    # 1. Global mass balance. Rebalance events are paired BY CONSTRUCTION -- one
    #    out and one in per move -- so the rebalance arms must match exactly.
    #
    #    Fleet entry and exit are deliberately UNPAIRED: a bike entering the
    #    system is a reb_in with no reb_out anywhere, and a retirement is the
    #    reverse. So the totals cannot be equal, and comparing them to each
    #    other makes the check print FAIL on a healthy ledger forever. The
    #    expected difference is entries minus exits -- the bikes that are still
    #    in service at the end of the window -- and THAT is what is asserted.
    totals = events.groupBy("event_type", "event_kind").count().collect()
    by_type = {}
    by_kind = {}
    for r in totals:
        by_type[r["event_type"]] = by_type.get(r["event_type"], 0) + r["count"]
        by_kind[r["event_kind"]] = by_kind.get(r["event_kind"], 0) + r["count"]

    reb_in, reb_out = by_type.get("reb_in", 0), by_type.get("reb_out", 0)
    entries, exits = by_kind.get("fleet_entry", 0), by_kind.get("fleet_exit", 0)
    expected = entries - exits
    actual = reb_in - reb_out

    status = "OK" if actual == expected else "FAIL"
    print(f"  mass balance   reb_in={reb_in:,} reb_out={reb_out:,}  [{status}]")
    print(f"    rebalance arms pair exactly; the {actual:,} difference is "
          f"fleet_entry {entries:,} - fleet_exit {exits:,} = {expected:,}")
    print(f"    i.e. {expected:,} bikes were still in service at the window end")
    if actual != expected:
        print("    !! PAIRING BUG: the rebalance arms do not match. A reb_out "
              "was emitted without its reb_in, or vice versa.")

    # 2. Stockout rate. With O(s,0) set to the minimum feasible value, every
    #    station touches zero at least once by construction -- but if a large
    #    FRACTION of hours sit at zero, the ledger is drifting downward and the
    #    gap rules are emitting too many reb_out events.
    per_station = (
        labelled.groupBy("station_id")
        .agg(F.count("*").alias("hours"),
             F.sum((F.col("occupancy") < 1.0).cast("int")).alias("zero_hours"))
        .cache()
    )
    totals_row = per_station.agg(
        F.sum("hours").alias("hours"), F.sum("zero_hours").alias("zero_hours")
    ).first()
    rate = 100.0 * totals_row["zero_hours"] / totals_row["hours"]
    status = "OK" if rate < 15 else "REVIEW"
    print(f"  hours at zero  {rate:.2f}%  [{status}]")

    # A headline rate above the threshold means one of two very different
    # things, and the headline alone cannot tell them apart:
    #
    #   CONCENTRATED -- a handful of stations sitting at zero for their whole
    #     history. Decommissioned docks and near-dead stations do exactly this,
    #     and it says nothing about the gap rules.
    #   SPREAD -- every station drifting down. THAT is the failure this check
    #     is looking for: too many reb_out events.
    #
    # So report the concentration, not just the rate.
    worst = per_station.orderBy(F.desc("zero_hours")).take(20)
    top_zero = sum(r["zero_hours"] for r in worst)
    share = 100.0 * top_zero / totals_row["zero_hours"] if totals_row["zero_hours"] else 0.0
    n_stations = per_station.count()
    print(f"    worst 20 of {n_stations} stations hold {share:.1f}% of all "
          f"zero-hours -- concentrated means dead stations, spread means drift")

    dead = per_station.filter(F.col("zero_hours") == F.col("hours")).count()
    print(f"    {dead} station(s) are at zero for their ENTIRE history")
    for r in worst[:5]:
        pct = 100.0 * r["zero_hours"] / r["hours"]
        print(f"      station {r['station_id']}: {r['zero_hours']:,}/{r['hours']:,} "
              f"hours at zero ({pct:.1f}%)")
    per_station.unpersist()

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
    zones = Zones(args.data_bucket, args.model_bucket)

    trips = spark.read.parquet(f"{zones.clean}/trips/")
    # clean/, not parsed/: the station table has been through phase 2, which is
    # what drops `Virtual Station`. Reading parsed/ here would put the
    # pseudo-station back into the grid below.
    stations = spark.read.parquet(f"{zones.clean}/stations/")

    window_end = trips.agg(F.max("end_time")).first()[0]
    print(f"[labels] window ends {window_end}")

    traj = bike_trajectories(trips)
    events = rebalancing_events(traj, cfg).unionByName(
        fleet_events(traj, cfg, window_end)
    ).cache()

    grid = station_hour_grid(spark, stations, trips, cfg)

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

    # 3.7 -- capacity, derived from the reconstruction itself. Historical
    # capacity is not published and stations have been resized, so there is no
    # external number to use. The rolling max tracks a resize.
    days = cfg["labels"]["capacity_rolling_days"]
    w_cap = (
        Window.partitionBy("station_id")
        .orderBy(F.col("hour_ts").cast("long"))
        .rangeBetween(-days * 86400, 0)
    )
    labelled = (
        labelled
        # Historical capacity is not published and stations have been resized,
        # so a single fixed number would misstate pct_full for earlier periods.
        # The rolling max tracks a resize; the floor of 1 keeps the division
        # below defined for a station that never held a bike.
        .withColumn("capacity", F.greatest(F.max("occupancy").over(w_cap), F.lit(1.0)))
        # 3.9 -- TIER-2 LABELS. Estimated, and reported as such.
        .withColumn("pct_full", F.col("occupancy") / F.col("capacity"))
        .withColumn("is_empty", F.col("occupancy") < 1.0)
        .withColumn("is_full", F.col("occupancy") >= F.col("capacity"))
    )

    ledger_diagnostics(labelled, events, cfg)

    # Cast to the types the Glue DDL publishes, rather than letting Spark's
    # inference decide. Parquet is self-describing so Spark reads either way,
    # but ATHENA reads the DDL -- and a bigint column declared int is not an
    # error there, it is a wrong number in a QA gate. The catalog is the
    # contract; this select is where the job conforms to it.
    out = (
        labelled.select(
            F.col("station_id").cast("int").alias("station_id"),
            F.col("hour_ts"),
            F.col("arr").cast("int").alias("arr"),
            F.col("dep").cast("int").alias("dep"),
            F.col("reb_in").cast("int").alias("reb_in"),
            F.col("reb_out").cast("int").alias("reb_out"),
            F.col("net_flow").cast("int").alias("net_flow"),
            F.col("occupancy").cast("double").alias("occupancy"),
            F.col("capacity").cast("int").alias("capacity"),
            F.col("pct_full").cast("double").alias("pct_full"),
            F.col("is_empty"),
            F.col("is_full"),
        )
        # Named part_* so the partition column cannot shadow the `month`
        # FEATURE that calendarfeat derives. Same value, different job: one is
        # a storage layout, the other is a model input, and letting a write
        # silently overwrite the second is how a feature quietly becomes an
        # int partition key.
        .withColumn("part_year", F.year("hour_ts"))
        .withColumn("part_month", F.month("hour_ts"))
    )

    (out.write.mode("overwrite").partitionBy("part_year", "part_month")
        .parquet(f"{zones.clean}/station_hour_labels/"))

    print(f"[labels] wrote {out.count():,} station-hours")
    return 0


if __name__ == "__main__":
    sys.exit(main())
