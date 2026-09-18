# Effects of External Factors on Bicycle Station Balance
## Indego Bikeshare, Philadelphia — Final Project Report

**Team 7** — Qianyu Chen, Boxuan Chen, Tong Wu, Caleb Okoh-Aihe
**Date** — 2026-09-18
**Deliverable** — SageMaker Model Registry package `station-balance/1`, AWS account 739638629450, us-east-1

> Every number in this report is measured from the pipeline run of 2026-09-18.
> None are estimated, and none are carried over from the project draft.
> Where a figure differs from the original proposal, the difference is stated
> rather than quietly corrected.

---

## 1. Motivation

The City of Philadelphia, with Qucit, algorithmically rebalances the Indego
bikeshare network — vans move bikes between stations to keep docks neither
empty nor full. Riders still routinely arrive at an empty station.

The rebalancing algorithm is proprietary, but the failure mode it exhibits is
public: stations empty out in patterns. If those patterns track **external
factors** — weather, time of day, day of week, street closures — then the
information needed to anticipate a stockout exists before the stockout happens,
and the question worth answering is *how much* each factor actually explains.

That "how much" is the whole point. A model that predicts availability well but
cannot say *why* tells an operator nothing they can act on. This project
therefore treats **attribution as the deliverable**, not as a diagnostic printed
after the real work.

---

## 2. Problem Statement and Objective

**Quantify the effect of external factors on dock availability at Indego
stations, per station, per hour.**

Concretely, the project produces a registered model bundle containing:

| Artifact | What it is |
|---|---|
| `model_net_flow.txt` | LightGBM regressor — predicts hourly net flow (arrivals − departures) |
| `model_is_empty.txt` | LightGBM classifier — probability a station is empty in a given hour |
| `features.json` | The feature contract: names, groups, and **order** the model was trained under |
| `metrics.json` | Test metrics, gate results, and the caveats that qualify them |
| `attribution.json` | **The research output** — mean absolute SHAP per factor group |

### What changed from the project draft

The draft proposed three factors and an interactive tool. The delivered project
measures two factors and registers a model. Both changes were forced by the
data, and both are documented rather than silently dropped.

| Draft | Delivered | Why |
|---|---|---|
| Weather, commuter patterns, **street closures** | Weather, temporal. **Closure arm cut** | PGW is the only source with real closure history and publishes each closure as a bare text address with no coordinates. Resolving ~108 K of those needs a street-name normaliser and a centerline range join that do not exist, against a fixed delivery date. A closure feature that is mostly noise is worse than a stated absence. |
| Interactive Vue3 tool | Model registered; serving drawn, not built | The research question is answered by the attribution, not by a UI. Serving adds an always-on cost profile to a project whose cost at rest is S3 storage. The serving design is specified in `docs/live_inference.drawio`. |
| Kafka / Hadoop / Spark-on-YARN | S3 / EMR Serverless / SageMaker | Every input is a historical archive. There is no stream to consume, so a streaming stack would have been operated for its own sake. See §5. |

**The closure arm being absent, rather than zero, is stated in `features.yaml`,
in every `metrics.json`, and here.** A column of zeros would have looked like a
measured non-effect. It is not one.

---

## 3. Data

### 3.1 Volume and variety

Three sources, three formats, three access patterns — the variety is genuine,
not decorative.

| Stage | Objects | Size | Rows |
|---|---|---|---|
| `raw/` — downloaded bytes, untouched | 59 | 109.5 MB | — |
| `parsed/` — Parquet, schema normalised | 40 | 115.6 MB | — |
| `clean/` — filtered, deduped, tz-localised | 5,159 | 273.7 MB | 5,096,494 trips |
| `training/` — wide table, chronologically split | 203 | 85.6 MB | **9,425,998** station-hours |

The grid holds **9,443,782** station-hours; **9,425,998** reach the training
splits. The 17,784-row difference is the lag warm-up — the first hours of each
station's window have no `net_flow_same_hour_last_week` to reference, and are
dropped rather than imputed.

Compressed archives expand roughly 2.5× into the analysis-ready grid: **103 MB
of quarterly ZIPs becomes 9.4 million labelled station-hours.**

| Dimension | Measured value |
|---|---|
| Trip records (post-filter) | **5,096,494** |
| Distinct bikes | **4,633** |
| Stations in the modelled grid | **354** (of 368 published) |
| Station-hours | **9,443,782** |
| Observation window | 2022-01-01 → 2026-07-02 (**4.5 years**, 39,436 hours) |
| Weather observations | 4 grid points × 39,436 hours × 9 variables |
| Features per row | **22**, in 4 groups |
| Mean occupancy | 6.01 bikes |
| Hours at zero occupancy | **5.65%** |
| `net_flow` exactly zero | **65.0%** — the single most consequential property of this data |

Ridership is strongly seasonal, which the temporal features exist to capture:

| Year | Q1 | Q2 | Q3 | Q4 |
|---|---|---|---|---|
| 2022 | 123,597 | 246,282 | 299,832 | 199,458 |
| 2023 | 161,216 | 281,036 | 339,408 | 262,611 |
| 2024 | 187,151 | 354,575 | 392,106 | 286,955 |
| 2025 | 192,897 | 349,795 | 445,611 | 312,850 |
| 2026 | 196,155 | 463,776 | — | — |

Q3 runs roughly 2.3× Q1 in every year, and the network grew ~50% over the
window. A random train/test split would leak both trends; §5.3 explains the
chronological split that prevents it.

### 3.2 Schemas — the three that matter

**Indego trip archive** — quarterly ZIP containing one CSV. The source of the
label, and the only source of station-level demand.

```
trip_id           bigint     unique per trip
duration_s        int        filtered to [60s, 24h]
start_time        timestamp  published as M/d/yyyy H:mm  ← see §4.2
end_time          timestamp
start_station_id  int
end_station_id    int
bike_id           string     the join key for the entire label reconstruction
bike_type         string     classic | electric
passholder_type   string
```

Shape: ~283,000 rows per quarter × 18 quarters. **Schema drifts across
quarters** — column names change between archives — so the mapping in
`pipelines/conform/job.py` is explicit and asserted: an unrecognised schema
stops the job rather than silently nulling a column.

**Open-Meteo Historical Forecast** — gzipped JSON, one file per grid point per
window, columnar (parallel arrays, not records).

```json
{ "latitude": 39.96, "longitude": -75.16, "timezone": "America/New_York",
  "hourly": { "time": ["2022-01-01T00:00", ...],       // 4,392 per file
              "temperature_2m": [9.8, ...],  "apparent_temperature": [9.4, ...],
              "precipitation": [0.0, ...],   "rain": [...], "snowfall": [...],
              "wind_speed_10m": [...],       "wind_gusts_10m": [...],
              "cloud_cover": [...],          "relative_humidity_2m": [...] },
  "daily":  { "time": [...], "sunrise": [...], "sunset": [...] } }
```

**Four grid points, not 354.** The reanalysis grid is ~11 km, so nearly every
Indego station falls in the same cell; per-station calls would return identical
data and burn quota. `sunrise`/`sunset` drive `is_daylight`.

**Indego station table** — a single CSV, 368 rows.

```
Station_ID, Station_Name, Day of Go_live_date, Status
3000,       "Virtual Station", 4/23/2015,      Active     ← not a physical dock
```

It publishes `Status` but **no retirement date**, which turns out to matter
(§4.4).

**Training wide table** — the 22 features, grouped because the groups *are* the
research question:

| Group | n | Features |
|---|---|---|
| `weather` | 9 | temperature_2m, apparent_temperature, precipitation, rain, snowfall, wind_speed_10m, wind_gusts_10m, cloud_cover, relative_humidity_2m |
| `temporal` | 8 | hour_sin, hour_cos, dow_sin, dow_cos, is_weekend, is_holiday, is_daylight, month |
| `station_static` | 1 | capacity |
| `lag` | 4 | occupancy_lag_1h, net_flow_lag_1h, net_flow_same_hour_last_week, net_flow_rolling_7d_mean |

Hour and day-of-week are encoded as sine/cosine pairs so that hour 23 and hour 0
are adjacent rather than maximally distant.

---

## 4. Accommodations Made to the Data

This section is the honest core of the report. Six substantive compromises were
required; each is a decision with a stated cost.

### 4.1 There is no historical occupancy data — the label had to be reconstructed

**The problem.** The project needs to know how full each station was, hour by
hour, for four years. No such archive exists. GBFS `station_status` publishes a
*live snapshot only* — query it today and you learn about today.

**The accommodation.** Reconstruct occupancy from trip data by chaining each
bike's trips on `bike_id`. If a bike's next trip *starts* at a different station
from where its last trip *ended*, a van moved it, and a `reb_out`/`reb_in` pair
is emitted. Summing arrivals, departures and rebalancing events per station-hour
yields a running ledger.

**The cost — two tiers of label with different trustworthiness:**

| Tier | Variable | Error |
|---|---|---|
| **Tier 1** | `net_flow = arrivals − departures` | **Zero.** Counted directly from trip endpoints |
| Tier 2 | `occupancy`, `pct_full`, `is_empty` | Estimated — a cumulative ledger plus a solved initial condition |

**Training uses Tier 1.** This is the single most important design decision in
the project: because `net_flow` is counted rather than reconstructed,
**reconstruction error cannot enter the learned weights**, and therefore cannot
contaminate the attribution result that is the deliverable. Tier 2 is retained
as an input feature and as the basis of the `is_empty` classifier, and is
reported with its uncertainty stated.

### 4.2 Timestamps parsed to NULL and the job still went green

**The problem.** On the first full run, every column name mapped correctly and
**100% of rows were silently dropped.** The archives publish timestamps as
`M/d/yyyy H:mm`; Spark's implicit `to_timestamp` returns NULL for that format,
and the subsequent window filter discarded every NULL row without comment. The
job succeeded, the output was empty, and nothing in the logs said why.

**The accommodation.** `assert_timestamps_parsed` in phase 2 — a hard gate that
fails the job above a 1% unparseable rate.

**Why it is a gate and not a warning:** the failure is *silent*. A loud failure
needs no gate; a silent one that produces a plausible-looking empty table is
exactly what a gate is for.

### 4.3 Occupancy level is a lower bound, not a measurement

**The problem.** The ledger gives the *shape* of occupancy over time but not its
*level* — `O(s,0)`, one unknown scalar per station. The original design pinned
it with `0 ≤ O ≤ capacity` and took the midpoint. That needs a published
capacity per station, and the only source for one is a live feed this project
does not read.

**The accommodation.** Use the lower constraint alone:
`O(s,0) = −min(cumulative delta)` — the smallest starting value that keeps
occupancy non-negative throughout. Capacity is then the rolling 90-day maximum
of the result, self-consistent by construction.

**The cost.** The level is a **lower bound**, not a centred estimate. The
*shape* of the series — which is what the features predict — is unaffected. Any
use of `pct_full` or `is_empty` as a measurement rather than an estimate is
unsupported, and `metrics.json` says so.

### 4.4 Thirteen stations that never existed in the window

**The problem.** The 3.10 diagnostic reported **17.62%** of station-hours at
zero occupancy, against a threshold of 15%. The breakdown showed it was
concentrated, not drift: **13 stations sat at zero for their entire 39,440-hour
history.** They have a go-live date before the window opens and not one trip
inside it. The station table publishes `Status` — 57 of 368 are `Inactive` — but
no retirement date, so a station decommissioned in 2019 still looked eligible
for every hour from 2022 onward.

**The accommodation.** Bound each station's grid by *observed activity* rather
than by go-live alone: `[max(go_live, first observed trip), last observed trip]`,
with an idle-vs-retired threshold reusing the same `fleet_exit_days` prior that
phase 3 uses to call a bike retired — one concept, one config key.

**The result:**

| | Before | After |
|---|---|---|
| Hours at zero occupancy | 17.62% | **5.65%** ✓ |
| Stations at zero for entire history | 13 | **0** |
| Stations in grid | 367 | 354 observed |

**The 15% threshold was not moved.** Moving a threshold to make a number pass is
how a check stops being one.

### 4.5 A pseudo-station in the station table

`Station_ID 3000, "Virtual Station"` is Indego's staff check-in/out placeholder,
not a physical dock. Phase 2 dropped it from *trips* but not from the *station
table*, so it entered the station-hour grid as a station with no dock. It is now
dropped from both. Note the trip archives carry **no station-name column**, so
it must be excluded by *id sourced from the station table*, not by name.

### 4.6 Rebalancing thresholds are priors, not findings

The gap rules (6h / 72h / 30d) that decide whether a bike gap is a van move, a
maintenance pull, or a retirement are **stated priors**. Validating them needs a
trip archive overlapping a window of recorded live dock counts, and this project
records none. Tier 2 therefore ships **diagnostically consistent but without a
numeric error bar**, and is reported that way rather than presented as measured.

What *is* validated is the emission **logic**: `tests/test_recovery.py` injects
synthetic van moves with known station pairs and timings, runs the actual 3.2/3.3
functions, and asserts exact recovery — including that two trips either side of
a quarter boundary at the same station produce **no** event, the specific failure
a per-quarter pass would introduce. It runs on the cluster, against the same
Spark version phase 3 uses.

---

## 5. Architecture — Before and After

### 5.1 Before (project draft)

```
Data Sources ──► Kafka ──► Spark Streaming ──► Hadoop/HDFS ──► Vue3 SPA
(Indego API, Weather API, Street Closures, Events)
```

Streaming ingest, a self-hosted distributed store, and an interactive front end.

### 5.2 After (delivered)

```
3 historical sources ──► Lambda ──► S3 (raw│parsed│clean)
      ──► EMR Serverless, phases 1-5 ──► training splits (S3)
      ──► SageMaker Processing, phase 6 ──► Model Registry
```

Full detail, with measured volumes and gate positions, in
`docs/pretrained_model.drawio`. The serving path that was *not* built is in
`docs/live_inference.drawio`; the Model Registry is the only link between them.

### 5.3 Why the architecture changed

| Draft component | Replaced by | Reason |
|---|---|---|
| **Kafka** | Lambda + S3 | Kafka exists to decouple producers from consumers in a stream. Every input here is a quarterly ZIP downloaded once. There is no stream. |
| **Hadoop / HDFS** | S3 + Glue Catalog | Cluster operation is effort spent on infrastructure rather than on the research question. The one stage that genuinely needs distribution — the global `bike_id` pass — runs fine serverless. |
| **Spark on YARN** | EMR Serverless | Same Spark, no cluster to operate. `preInitializedCapacity = 0`, so an idle pipeline costs nothing. |
| **Vue3 SPA** | Model Registry | See §2. |
| **Step Functions** *(considered)* | Numbered shell scripts | Orchestration makes *unattended* re-runs reliable. Every run here is attended, and a numbered script lets you stop halfway and inspect. |
| **Apache Sedona** *(considered)* | — | Its justification was a row explosion from expanding closure permits to hourly intervals. After the closure arm was cut, there is no spatial join left. |

**Nothing runs on a schedule.** There are zero EventBridge rules in the account,
and `scripts/00_preflight.sh` asserts that count is zero.

### 5.4 The six phases

| Phase | Does | The trap it avoids |
|---|---|---|
| **1 · Land** | Archives → Parquet | Nothing is cleaned here, so a bad filter rule is fixed without re-downloading 103 MB |
| **2 · Conform** | Schema map, tz-localise, filter, dedupe | DST breaks any naive local-time join; the `M/d/yyyy H:mm` NULL trap (§4.2) |
| **3 · Labels** | Trajectories, rebalancing events, ledger | **Must be a single global pass** — partitioning by quarter injects a false rebalance every 3 months |
| **4 · Features** | Weather join, calendar, lags | The leakage audit is a hard stop, not a warning |
| **5 · Assembly** | Wide table, chronological split | **Never random-split** — it leaks future weather and inflates metrics |
| **6 · Train** | Baseline, regressor, classifier, evaluate, attribute | Evaluate on test exactly once |

---

## 6. Tools

| Tool | Role | Why this one |
|---|---|---|
| **Amazon S3** | Data lake — `raw` / `parsed` / `clean` zones | Durable, versioned, and the zone split makes reprocessing cheap |
| **AWS Lambda** | Ingest: trips, stations, weather | Three short download tasks. No server, no schedule |
| **EMR Serverless** (PySpark) | Phases 1-5 | Spark without a cluster to operate; scales to zero between attended runs |
| **AWS Glue Data Catalog** | Table metadata | Hand-written DDL, no crawler — a convenience over Parquet paths, not a dependency |
| **Amazon Athena** | QA gates, ledger diagnostics | Ad-hoc SQL against the lake with no infrastructure |
| **SageMaker Processing** | Phase 6 execution | Ran the training; see §7.1 for why Processing and not Training |
| **SageMaker Model Registry** | The deliverable | Versioned, approval-gated handoff point |
| **LightGBM 4.5.0** | Both models | Gradient boosting on tabular data with mixed feature scales; fast on CPU |
| **SHAP 0.46.0** | Attribution | Per-feature contributions that aggregate cleanly to the factor groups |
| **Terraform 1.12.2** | All infrastructure | Three layers — storage / ingestion / pipeline. No console clicks |
| **CloudWatch** | Logs, monthly budget alarm | Cost guardrail on a student account |

---

## 7. Experiments

Four training runs. The first is the deliverable; the rest exist because the
first produced a result that did not survive scrutiny.

| # | Run | Objective | Features | best_iter | Test MAE | Weather share | Status |
|---|---|---|---|---|---|---|---|
| 1 | `20260918T190026Z` | L1 | all 22 | 553 | **0.5070** | 0.4% | **Deliverable** — `station-balance/1` |
| 2 | `nolag-192421Z` | L1 | −lag | **1** | 0.5267 | 0.0% | **Void** — degenerate fit |
| 3 | `l2full-193235Z` | L2 | all 22 | 1837 | 0.5393 | **5.4%** | Attribution instrument — `/2` |
| 4 | `l2nolag-193255Z` | L2 | −lag | 15 | 0.5444 | 3.0% | Attribution instrument — `/3` |

### 7.1 Experiment 0 — an infrastructure constraint, not a modelling one

`CreateTrainingJob` failed with `ResourceLimitExceeded`: the account's quota for
`ml.m5.4xlarge for training job usage` is **0**. Checking all 2,093 SageMaker
quotas showed **every** training-job and spot-training-job quota at zero, in
every region — an account-level restriction, with a Support case open and no ETA.

Processing-job quota for `ml.t3.xlarge` was **2**. Processing and Training are
the same service, the same execution role, and the same container image — only
the quota differs. Phase 6 therefore runs as a Processing job with
**`train.py` unmodified**: it resolves its channels from `SM_CHANNEL_*` and
`SM_MODEL_DIR`, so redirecting those to the processing mount paths is the entire
adaptation. The entrypoint tars the bundle to the same S3 path, so the publish
step cannot tell the difference. Run time: 18 minutes, ~$0.20.

### 7.2 Experiment 1 — the deliverable, and a result that looked wrong

The full L1 model passed its gate: MAE **0.5070** against a (station, hour,
weekday) mean baseline of **0.5581**. But the attribution read:

| Group | Share |
|---|---|
| lag | 64.2% |
| temporal | 30.8% |
| station_static | 4.7% |
| **weather** | **0.4%** |

Weather — one of the two surviving research factors — at 0.4%. Published
unqualified, that reads as *"weather does not affect bike availability."*

### 7.3 Experiment 2 — the hypothesis, and why it was wrong

**Hypothesis:** the lag features absorb weather's signal. Weather is
autocorrelated at exactly those lags — last Tuesday 5pm was also cold and wet —
so SHAP credits the lag, and weather keeps only the residual. Removing the lags
should reveal a larger *total* effect.

The no-lag run returned weather at **0.0%** — apparently confirming that weather
does nothing at all.

**It confirmed nothing. The run was void.** All nine weather features came back
at *exactly* 0.000000, along with four temporal features, and total attributed
effect collapsed from 0.115 to 0.00066. `best_iteration` was **1**. The model
was a single tree.

**Diagnosis, verified against the data rather than assumed:** an Athena query
over the training split confirmed the weather columns carry real variance
(`temperature_2m` spans −13.6 to 38.1, sd 9.85; `precipitation` sd 1.89). Not a
data bug. The cause is that **65.0% of `net_flow` values are exactly zero**, and
the objective was `regression_l1`, whose optimal constant is the **median —
which is zero**. With the lags present, something shifts that median and the
model trains to 553 rounds. Strip them and nothing does, so early stopping fired
at iteration 1.

**The instrument collapsed; it did not return a zero.** That distinction is the
difference between a measurement and a broken measurement that looks like one.

### 7.4 Experiments 3 and 4 — L2, and the finding that mattered

Both re-run under `--objective l2`, whose optimal constant is the mean, which a
zero-inflated target *does* move. Full-features and no-lag, so the pair differs
in one thing at a time.

| Run | best_iter | weather \|SHAP\| | share | total \|SHAP\| |
|---|---|---|---|---|
| L1 full | 553 | 0.000478 | 0.4% | 0.114990 |
| L1 no-lag | 1 | 0.000000 | 0.0% | 0.000664 |
| **L2 full** | **1837** | **0.026006** | **5.4%** | **0.479613** |
| L2 no-lag | 15 | 0.001754 | 3.0% | 0.057477 |

**The mediation hypothesis is disproved.** Weather's absolute effect is *larger*
with the lags present (0.026) than without them (0.0018). Removing the lags does
not liberate weather's contribution — it destroys the model's ability to fit
anything.

**What the experiments actually found is more useful: the objective was
suppressing the entire attribution.** On identical features, L2 fits 3.3× more
total effect than L1 and runs 1837 rounds against 553. **Weather at "0.4%" was
never a statement about weather. It was a statement about fitting the median of
a target that is 65% zeros.**

### 7.5 What this changed in the code

A stump's SHAP values read as "no effect" when they mean "never fitted", and
this project nearly shipped that error. `train.py` now flags
`best_iteration <= 5` as a **DEGENERATE FIT** in the log, and records
`gates.fit_not_degenerate` in `metrics.json` and `degenerate_fit` in
`attribution.json`.

---

## 8. Results and Deliverables

### 8.1 Model performance — `station-balance/1`

| Metric | Model | Baseline | Verdict |
|---|---|---|---|
| `net_flow` MAE | **0.5070** | 0.5581 | **9.2% better** ✓ |
| `net_flow` RMSE | 1.1111 | — | — |
| `is_empty` PR-AUC | 0.8716 | — | — |
| `is_empty` Brier | 0.0243 | — | — |

The baseline is a (station, hour, weekday) mean — deliberately strong and cheap.
If the full model could not beat it, weather and time would be contributing
nothing, and that needed to be known early: the training flow routes that
failure back to phase 4, not forward to publish.

### 8.2 The attribution — the research answer

From `station-balance/2` (L2 full), mean absolute SHAP over a 20,000-row test
sample:

| Factor group | mean \|SHAP\| | Share |
|---|---|---|
| lag (the station's own recent history) | 0.275912 | 57.5% |
| **temporal** | 0.117231 | **24.4%** |
| station_static (capacity) | 0.060464 | 12.6% |
| **weather** | 0.026006 | **5.4%** |

**Time of day and day of week outweigh weather by roughly 4.5 to 1.**

Both external factors are dwarfed by the station's own recent history, which is
expected and is *not* a finding about external factors —
`net_flow_same_hour_last_week` is the single largest feature in every run. Bike
demand is overwhelmingly habitual: the best predictor of this Tuesday at 5pm is
last Tuesday at 5pm.

Weather's 5.4% is small but real, and it is the honest number. A practical
reading: **weather is a second-order correction to a strongly periodic demand
signal.** An operator anticipating stockouts should schedule against the clock
first and adjust for weather second.

### 8.3 Why the deliverable and the attribution come from different models

This is the one seam in the project, and it is deliberate:

- **`station-balance/1` (L1)** is the registered, approved, served model. It
  wins on MAE (0.5070 vs 0.5393), which is what choosing `net_flow` as the
  target implies.
- **`station-balance/2` (L2)** backs the attribution, because a model that fits
  4× more of the signal is the better instrument for asking which features carry
  it.

Both are in the registry. The instruments are registered
`PendingManualApproval` and described *do-not-serve*, and
`40_publish_model.sh --attribution-only` is what keeps them out of
`models/current/` — it classifies a variant by dropped groups **or** objective,
because an L2 full-feature run drops nothing and a dropped-groups-only test
would have waved it straight over the deliverable.

### 8.4 Registry state

| Version | Job | Approval | Role |
|---|---|---|---|
| 1 | `station-balance-20260918T190026Z` | Approved | Deliverable — L1, all features, `models/current/` |
| 2 | `station-balance-l2full-193235Z` | PendingManualApproval | Attribution instrument — L2, all features |
| 3 | `station-balance-l2nolag-193255Z` | PendingManualApproval | Attribution instrument — L2, no-lag (weakly fitted) |

### 8.5 Quality gates — all passed

| Gate | Step | Result |
|---|---|---|
| Timestamp parse rate | 2.2 | Pass — hard-fails above 1% unparseable |
| Synthetic recovery test | 3.2/3.3 | Pass — on the cluster, exact recovery of injected van moves |
| Global mass balance | 3.10 | Pass — `Σ reb_in = Σ reb_out` by construction |
| Hours at zero occupancy | 3.10 | **5.65%**, threshold 15% |
| Leakage audit | 4.8 | Pass — hard-fails the job |
| Chronological split | 5.2 | Enforced |
| Beats baseline | 6.1 | Pass — 9.2% |
| Fit not degenerate | 6.2 | Pass — 553 iterations (added after experiment 2) |

### 8.6 Cost

Roughly **$0.30** for all four training runs; **$1–2** per full pipeline run on
EMR Serverless; **under $1/month** at rest. Every meaningful cost risk is
something *left running* — a NAT gateway, a managed MLflow server,
pre-initialised EMR capacity, an inference endpoint. None are provisioned, and
the account was verified clean after the final run: zero jobs, zero endpoints,
zero notebooks, EMR stopped, zero schedules.

### 8.7 Limitations

1. **Tier-2 occupancy is unvalidated.** The level is a lower bound and the gap
   thresholds are stated priors. Fine for the deliverable — attribution rests on
   Tier 1 — but `pct_full` and `is_empty` are estimates, not measurements.
2. **Two factors, not three.** The absent closure arm is a real candidate
   explanation for weather's modest share.
3. **The attribution depends on the loss function.** Weather reads 0.4% under L1
   and 5.4% under L2 on identical features. The L2 number is better founded, but
   the sensitivity is itself a caveat worth carrying.
4. **The lag group is not an external factor.** It dominates the attribution and
   should not be read as an answer to the research question.
5. **Nothing serves this model.** The serving path is designed, not built.

---

## 9. Presentation Notes

**Lead with the reversal, not the pipeline.** The strongest material in this
project is that the first weather number was wrong and the team caught it. Open
on "we measured weather at 0.4%, didn't believe it, and found the objective was
hiding the signal." That is a better opening than an architecture diagram.

**Be precise about the no-lag experiment.** It is tempting to present it as
confirming weather does nothing. It does not: `best_iteration = 1` means the
model never fitted. Present it as a *void run* and as the reason the
degenerate-fit gate now exists. Anticipate the question "how do you know the
data wasn't broken?" — the answer is the Athena variance query in §7.3.

**Own the scope cuts early.** The closure arm and the Vue3 tool are both absent
from the draft's promises. State both in the objectives slide with the reason,
rather than letting someone notice their absence later.

**Expect: "isn't 5.4% just small?"** Yes — and that is the finding. The honest
framing is that demand is overwhelmingly habitual, and weather is a second-order
correction. The 4.5:1 ratio of temporal to weather is more quotable than either
number alone.

**Expect: "why is `lag` in the model at all if it isn't an external factor?"**
Because removing it produces a worse model *and* a worse attribution — that is
experiment 4. It is a control, not a competitor.

**Expect: "why two models?"** Answer in one sentence: L1 predicts better, L2
measures better, both are registered, only one is served.

**Have these numbers memorised:** 5.1 M trips · 9.4 M station-hours · 354
stations · 4.5 years · 22 features · 65% zeros · MAE 0.5070 vs 0.5581 baseline ·
temporal 24.4% vs weather 5.4%.

**Do not oversell the infrastructure.** The quota workaround (Training →
Processing) is a good engineering anecdote but a one-slide aside, not a theme.

---

## Appendix — Reproducing this

```bash
cd infra && ./deploy-all.sh --profile <aws-profile>

scripts/00_preflight.sh
scripts/10_ingest.sh
scripts/20_run_phase.sh land
scripts/20_run_phase.sh conform
scripts/25_run_gate.sh                  # GATE before labels
scripts/20_run_phase.sh labels
scripts/20_run_phase.sh features
scripts/20_run_phase.sh assembly
scripts/31_train_processing.sh          # 30_train.sh if training quota > 0
scripts/40_publish_model.sh <job-name>

# attribution instruments
TRAIN_ARGS="--objective l2" scripts/31_train_processing.sh l2full
scripts/40_publish_model.sh --attribution-only <job-name>
```

Decision rationale, rejected alternatives, and every failure encountered are
recorded in `.claude/decisions.md`.
