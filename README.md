# Indego Station Capacity Prediction

Modelling the effect of **weather** and **time of day / day of week** on dock capacity at Indego bikeshare stations in Philadelphia.

This repository builds a **pre-trained model** from historical data. Nothing in it reads a live feed, serves a prediction, or runs on a schedule. Real-time inference is a design deliverable only — see [`docs/live_inference.drawio`](docs/live_inference.drawio).

---

## 1. Objective

Quantify how external factors drive dock availability at each Indego station:

1. **Weather** — temperature, precipitation, wind, cloud cover
2. **Temporal** — hour of day, day of week, holidays, daylight

The deliverable is a **registered model bundle**: the trained model, the feature
contract it was trained under, its test metrics, and the SHAP attribution per
factor group. Attribution is the research question, not a by-product of
prediction.

### Street closures — cut

A third factor, permitted lane closures near the station, was scoped and then
dropped. PGW is the only source with real closure history, and it publishes
each closure as a bare text address with no coordinates. Turning ~108 K of
those into points needs a street-name normaliser and a street-centerline range
join that do not exist, and the work is open-ended against a fixed date.

Rather than ship a closure feature that is mostly noise, the arm is cut and
this project measures **two factors, not three**. That is stated here, in
`features.yaml`, and in every model's `metrics.json`, rather than implied by a
column of zeros. The rationale is recorded in `.claude/decisions.md`.

---

## 2. Architecture

Two diagrams, deliberately separate, because they describe two different things:

| File | What it is |
|---|---|
| **[`docs/pretrained_model.drawio`](docs/pretrained_model.drawio)** | **Built and deployed.** The batch training pipeline: download archives → process → train → register. Every input is historical. |
| **[`docs/live_inference.drawio`](docs/live_inference.drawio)** | **Not built.** What would exist if real-time inference were added, and how live data would interact with the trained model. |

Open either in the draw.io desktop app, <https://app.diagrams.net>, or the VS
Code *Draw.io Integration* extension.

The split is the point. Everything that touches a live feed — GBFS
`station_status`, the live weather forecast, DynamoDB, API Gateway, CloudFront —
lives in the inference diagram and **only** there. The training pipeline has no
live input, no schedule, and no serving path. Keeping them in one picture is
what let live-data machinery creep into a historical-attribution project in the
first place.

The only thing crossing from one to the other is the model bundle itself.

Three constraints the training diagram exists to make explicit:

| Constraint | Why it matters |
|---|---|
| Storage is drawn as prefixes with `EMR Serverless` looping through them | The read-transform-write cycle is where phases 2–5 actually live; one opaque store hides it |
| Nothing is on a schedule | Every run is attended. There is no EventBridge resource in the account, and `scripts/00_preflight.sh` asserts that |
| The model is registered, not served | The deliverable is the bundle in the SageMaker Model Registry. No endpoint, no API, nothing warm |

**Sizing and cost.** EMR Serverless at driver 4 vCPU / 16 GB, executors 4 × 4 vCPU, `preInitializedCapacity` **0** — ~$1–2 per pipeline run. SageMaker training on one `ml.m5.4xlarge`, ~$0.25 per run. At rest this is S3 storage and nothing else, well under $1/month. Every meaningful cost risk is something *left running*: a NAT Gateway ($32/mo), a managed MLflow tracking server (~$460/mo), or pre-initialised EMR capacity. None are provisioned.

---

## 3. Scale and Tooling Rationale

State this explicitly, because it will be questioned.

Figures below are **measured**, not estimated — trip counts derived from the published archive sizes on 2026-09-17 (see `indego_capacity_data_sources.csv`).

| Quantity | Measured |
|---|---|
| Trip records, 2022 Q1 – 2026 Q2 | **5.16 M** (18 quarterly archives, 107 MB zipped / ~680 MB CSV) |
| Trip *events* (arrivals + departures) | ~10.3 M |
| Stations | ~250 |
| Hours in window | ~39,000 |
| Station-hour modelling grid | **~10 M rows** (~1–2 GB Parquet at ~60 features) |
| Total storage footprint | **~3–5 GB** |

This is a **modest** volume. The entire pipeline fits in RAM on a single 16 GB machine.

Distributed processing is justified by:

1. **Step 3.1** — a global sort-and-window over the entire trip history partitioned by `bike_id`. This is the genuine shuffle-heavy stage and the peak memory consumer. Note that 5.16 M rows is not, on its own, Spark-scale.
2. **The architectural requirement of the project itself.** This is the honest primary justification and should be stated as such.

It is **not** justified by raw input size. Be honest about this rather than overstating the data volume — a reviewer who checks the numbers should find them conservative, not inflated.

---

## 4. Data Sources

Full machine-readable inventory: **`indego_capacity_data_sources.csv`** (17 sources, endpoints, formats, auth requirements, caveats). Three of them are used.

| Source | Role |
|---|---|
| Indego Trip Data (quarterly CSV) | Complete event log of ridden movement. Basis of all label construction. |
| Indego Station Table (CSV) | `go live date`, required in 3.4 so pre-launch stations are not scored as zero-demand |
| Open-Meteo Historical Forecast | Weather features for the training window |

Everything else in the inventory is either a **live feed** — GBFS
`station_status`, GBFS `station_information`, the live Open-Meteo forecast, the
current-state ArcGIS closure layer — or part of the cut closure arm. Live feeds
belong to `docs/live_inference.drawio`. None is downloaded here.

All three retained sources are `Auth Required: No`, which is why there is no
Secrets Manager in the architecture.

### Two gaps worth stating plainly

**There is no published historical archive of dock-level occupancy.** GBFS
`station_status` is a live snapshot only. The label must therefore be
reconstructed from trip data — see section 5.

**There is no published historical capacity either.** Stations have been
resized and only the current dock count is available, from a live feed this
project does not use. Capacity is therefore derived from the reconstructed
occupancy itself (step 3.7), which is self-consistent but means `pct_full` is
an estimate, not a measurement.

---

## 5. Method

### The label problem

There is no published historical archive of dock-level occupancy — GBFS `station_status` is a live snapshot only. The label is therefore reconstructed from trip data by chaining each bike's trips on `bike_id`: where a bike's next trip starts at a different station from where its last trip ended, a van moved it, and a `reb_out` / `reb_in` pair is emitted.

This yields **two tiers of label**:

| Tier | Variable | Error |
|---|---|---|
| **Tier 1** | `net_flow(s,t)` = `arr − dep` | **Zero.** Counted directly from trip endpoints |
| Tier 2 | `O(s,t)`, `pct_full`, `is_empty` | Estimated — a cumulative ledger plus a solved initial condition |

**Train on Tier-1 `net_flow`.** It is counted rather than reconstructed, so reconstruction error never enters the learned weights, and predicting the *change* is exactly what weather and time drive. Tier 2 is retained as an input feature and as the basis of the `is_empty` classifier.

`O(s,0)` is one unknown scalar per station. The original design pinned it with `0 ≤ O ≤ capacity`, taking the midpoint of the feasible interval — but that needs a published capacity, and the only source was a live feed. So the lower constraint alone is used: `O(s,0) = −min(cumulative delta)`, the smallest start that keeps occupancy non-negative. **The level is therefore a lower bound rather than a centred estimate. The shape of the series, which is what the features predict, is unaffected.** Capacity is then the rolling 90-day maximum of the result, which is self-consistent by construction.

### The pipeline

Six phases, run in order, by hand.

| Phase | Does | Watch out for |
|---|---|---|
| **1 · Land** | Trip ZIPs, station CSV, weather JSON → Parquet | Nothing is cleaned here; that is phase 2's job, so a bad filter rule can be fixed without re-downloading |
| **2 · Conform** | Normalise schema, localise timestamps, filter, dedupe | DST breaks any naive local-time join; `Virtual Station` rows corrupt mass balance |
| **3 · Labels** | Bike trajectories, rebalancing events, station-hour grid, `net_flow`, occupancy ledger | Must be a **single global pass** — partitioning by quarter injects a false rebalance every 3 months |
| **4 · Features** | Weather join, calendar features, lags | The leakage audit is a hard stop, not a warning |
| **5 · Assembly** | Wide table, **chronological** split, class weights | Never random-split — it leaks future weather and inflates metrics |
| **6 · Train** | Baseline, `net_flow` regressor, `is_empty` classifier, tune, evaluate, attribute | Evaluate on test exactly once; attribution is a deliverable, not a by-product |

Phases 1–5 run on EMR Serverless; phase 6 on SageMaker. Ordering comes from the numbered scripts in `scripts/`, not from an orchestrator — every run is attended, and a numbered script lets you stop halfway and inspect.

### No inference pipeline

There is no second pipeline. The model is registered and that is the end of the deliverable.

What a live inference path would have to honour — the identical feature list and order from `features.json`, calendar features from `pipelines/calendarfeat.py` rather than a reimplementation, and live `station_status` supplying only the integration constant `O(s,t₀)` — is drawn in `docs/live_inference.drawio`. `calendarfeat.py` is deliberately Spark-free so that path could import the same file.

---

## 6. Quality Gates

### Ledger diagnostics (step 3.10)

| Check | Expected | Failure meaning |
|---|---|---|
| Global mass balance | `Σ reb_in == Σ reb_out` for rebalancing events, exactly, by construction | Pairing logic bug. Fleet entry/exit are deliberately unpaired, so a difference equal to the fleet event count is expected |
| Hours at zero occupancy | Low — well under 15% | The ledger is drifting downward and the gap rules are emitting too many `reb_out` events |
| Out-of-system fleet count over time | Smooth few-% of fleet with a maintenance-shaped seasonal bump | A sawtooth means the 6h / 72h thresholds are mis-set |

### Recovery test (steps 3.2 / 3.3) — a gate, not a nice-to-have

The ledger diagnostics above check internal consistency; they do not check that the event-emission rules are *correct*. `tests/test_recovery.py` does, without external data: it injects synthetic van moves with known station pairs and timings, runs the **actual** 3.2 / 3.3 functions imported from `pipelines/labels/job.py`, and asserts exact recovery of every injected move. It also asserts that two trips either side of a quarter boundary at the same station produce **no** event — the specific failure a per-quarter pass would introduce.

It needs Spark, so it skips in a plain Python environment. **That skip is not a pass.** Run it before trusting phase 3 output:

```bash
spark-submit tests/test_recovery.py
```

This validates the emission **logic**. The 6h / 72h / 30d thresholds are **priors** and are not validated: doing so needs a trip archive overlapping a window of recorded live dock counts, and this project records none. **Tier 2 therefore ships diagnostically consistent but without a numeric error bar, and is reported that way** — `metrics.json` says so explicitly. Tier-1 `net_flow` is counted rather than reconstructed, so training, evaluation and attribution are unaffected.

### Modelling gates

- Chronological split only (5.2)
- Leakage audit passed for every feature (4.8) — this one **hard-fails the job**, because the failure it prevents is silent
- Full model beats the (station, hour, weekday) mean baseline (6.1)

---

## 7. Highest-Risk Items

1. **Step 3.1 — the global `bike_id` pass.** The one stage where a partitioning mistake produces plausible-looking but wrong output that no downstream check will obviously catch. Mitigated by the recovery test in §6, which is why that test is a gate.
2. **Schema drift across quarters.** 18 quarterly archives with drifting column names. The mapping in `pipelines/conform/job.py` is explicit and **asserted** — an unrecognised schema stops the job rather than silently nulling a column. Expect to add aliases on the first run.
3. **Tier-2 occupancy is unvalidated.** The level is a lower bound and the gap thresholds are stated priors. This is fine for the deliverable — attribution rests on Tier-1 — but any use of `pct_full` or `is_empty` as a measurement rather than an estimate is unsupported.
4. **Two factors, not three.** The closure arm is cut (§1). If the attribution result is weaker than hoped, the absent third factor is a real candidate explanation and should be named as one.

---

## 8. Repository Layout

```
.
├── README.md                           # This file — scope, sources, method
├── indego_capacity_data_sources.csv    # Source inventory with verified coverage
├── features.yaml                       # The feature contract (step 0.4)
├── docs/
│   ├── pretrained_model.drawio         # BUILT — the training pipeline
│   └── live_inference.drawio           # NOT BUILT — what serving would look like
├── infra/                              # Terraform, three layers + deploy scripts
│   ├── storage/                        # Two S3 buckets, Glue catalog, Athena, budget
│   ├── ingestion/                      # The ingest Lambda. No schedules.
│   ├── pipeline/                       # EMR Serverless, SageMaker role, model registry
│   └── deploy-*.sh                     # One per layer, plus deploy-all.sh
├── src/lambdas/
│   ├── common/lakeio.py                # Fetch + S3 helpers
│   └── ingest/handler.py               # trips | stations | weather
├── pipelines/                          # EMR Serverless Spark jobs + SageMaker entrypoint
│   ├── lib.py                          # Session, storage paths, config loading
│   ├── calendarfeat.py                 # Step 4.6 — Spark-free, so inference could reuse it
│   ├── land/                           # Phase 1 — archives → Parquet
│   ├── conform/                        # Phase 2
│   ├── labels/                         # Phase 3
│   ├── features/                       # Phase 4
│   ├── assembly/                       # Phase 5
│   └── training/                       # Phase 6
├── scripts/                            # The attended running order, numbered
└── tests/
    ├── test_recovery.py                # The 3.2/3.3 recovery GATE (needs Spark)
    └── test_calendarfeat.py            # Shared calendar features
```

### Running it

`infra/README.md` has the detail. The short version:

```bash
cd infra && ./deploy-all.sh          # storage → ingestion → pipeline
```

Nothing starts running — there is no schedule in this project. Drive the
phases by hand:

```bash
scripts/00_preflight.sh                  # did the deploy land?
scripts/10_ingest.sh                     # download trips, stations, weather
scripts/20_run_phase.sh land             # archives → Parquet
scripts/20_run_phase.sh conform          # normalise, filter, dedupe
scripts/20_run_phase.sh labels           # bike_id trajectories → net_flow
scripts/20_run_phase.sh features         # weather + calendar + lags
scripts/20_run_phase.sh assembly         # wide table, chronological split
scripts/30_train.sh                      # train, evaluate, attribute
scripts/40_publish_model.sh <job-name>   # register the model
```

Roughly 2–3 hours end to end, most of it unattended. `scripts/run_tests.sh`
runs the unit tests; the recovery gate needs `spark-submit` (§6).

---

## 9. Open Parameters

Unresolved values that affect implementation. Everything else is decided; the
reasoning and the rejected alternatives are recorded in `.claude/decisions.md`.

| Parameter | Current position |
|---|---|
| Gap thresholds 6h / 72h / 30d | Ship as stated priors — no validation set exists (§6) |
| Training window start | 2022 Q1 — consistent schema, post-COVID regime, e-bikes present |
| Censoring treatment | Observed flow understates demand at saturated stations. Options: censored regression (Tobit), or train only on non-saturated hours |
| Weather grid point | One point for the whole city. The reanalysis grid is ~11 km and the station footprint is smaller than one cell, so more points would repeat the same numbers |
