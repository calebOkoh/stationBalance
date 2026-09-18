# Indego Station Capacity Prediction

Modelling the effect of **weather**, **time of day / day of week**, and **street closures** on dock capacity at Indego bikeshare stations in Philadelphia.

---

## 1. Objective

Quantify and predict how three external factors drive dock availability at each Indego station:

1. **Weather** — temperature, precipitation, wind, cloud cover
2. **Temporal** — hour of day, day of week, holidays, daylight
3. **Street closures** — permitted lane closures and utility work near the station

The deliverable is twofold:

- **Attribution** — the measured effect size of each factor group (this is the research question, not a by-product of prediction)
- **Prediction** — for a given station and hour, the probability that the station is empty or full

### Downstream consumer

A web tool (out of scope for this repository, documented here only to constrain the inference contract) will render a map of Indego stations showing current capacity. Clicking a station reveals a **capacity tendency graph**: probability of being empty as a function of time of day, conditioned on the model's factors. The web tool is a visualisation layer only — all logic lives in the inference pipeline.

---

## 2. Architecture

The deployed architecture is a single draw.io diagram: **[`docs/aws_architecture.drawio`](docs/aws_architecture.drawio)**.

It uses draw.io's official AWS shape library (`mxgraph.aws4.*`), so every node carries its real service icon and category colour, and is saved uncompressed so it diffs as plain XML in review. Open it with the draw.io desktop app, <https://app.diagrams.net>, or the VS Code *Draw.io Integration* extension.

```bash
# export a static copy if one is needed
drawio -x -f svg -o docs/aws_architecture.svg docs/aws_architecture.drawio
```

Three constraints the diagram exists to make explicit:

| Constraint | Why it matters |
|---|---|
| The lake is drawn as zones with `EMR Serverless` looping through them | The read-transform-write cycle is where phases 2–5 actually live; one opaque store hides it |
| `station_status` is dashed into training | It is ground truth for ledger validation (step 6.6) **only** — never a feature. The easiest constraint in this project to violate by accident |
| The model is not on the request path | Inference writes a precomputed cube to DynamoDB on a schedule; API Gateway reads DynamoDB. ~250 stations × 48 hours ≈ 12 K rows, so enumerating beats serving live and a failed run degrades to stale data, not a 5xx |

**Sizing and cost.** EMR Serverless at driver 4 vCPU / 16 GB, executors 4 × 4 vCPU, `preInitializedCapacity` **0** — ~$1–2 per pipeline run. SageMaker training on one `ml.m5.4xlarge`, ~$0.25 per run. Steady state is under $5/month. Every meaningful cost risk is something *left running*: a NAT Gateway ($32/mo), a managed MLflow tracking server (~$460/mo), or pre-initialised EMR capacity. Keep Lambdas out of a VPC and none apply.

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
| PGW closure permits in window | ~108 K |
| `station_status` poller output | **72 K rows/day → ~26 M rows/year** |
| Total lake footprint | **~3–5 GB** |

This is a **modest** volume. The entire pipeline fits in RAM on a single 16 GB machine, and the largest row generator is not the trip history but the `station_status` poller, which grows without bound.

Distributed processing is justified by:

1. **Step 3.1** — a global sort-and-window over the entire trip history partitioned by `bike_id`. This is the genuine shuffle-heavy stage and the peak memory consumer. Note that 5.16 M rows is not, on its own, Spark-scale.
2. **The architectural requirement of the project itself.** This is the honest primary justification and should be stated as such.

~~Closure row multiplication~~ — **withdrawn.** This previously cited step 4.3/4.4 expanding permits to hourly intervals. That held for the Streets Dept layer, whose permits span months (~5,500 hours each; 8,481 permits would have expanded to ~46 M rows). Following the move to PGW as the primary closure history (§4), permits are **hour-scale** — typically one to four hours — so ~108 K permits expand to only ~430 K closure-hours, of which roughly 5% survive the 150 m station buffer. The row explosion does not occur.

It is **not** justified by raw input size. Be honest about this rather than overstating the data volume — a reviewer who checks the numbers should find them conservative, not inflated.

---

## 4. Data Sources

Full machine-readable inventory: **`indego_capacity_data_sources.csv`** (16 sources, endpoints, formats, auth requirements, caveats).

Summary of the primary sources:

| Source | Role |
|---|---|
| Indego Trip Data (quarterly CSV) | Complete event log of ridden movement. Basis of all label construction. |
| Indego GBFS `station_information` | Station capacity, latitude/longitude |
| Indego GBFS `station_status` | Live occupancy. Inference input + the only ground truth for validation. |
| Open-Meteo Historical Forecast | Training weather features |
| Open-Meteo Forecast | Inference weather features |
| **PGW Street Lane Closures** | **PRIMARY closure history.** Gas-main utility closures via SOAP `GetEUNHistory`. Verified dense coverage December 2009 → present. Addresses only — no geometry (see 2.6) |
| Philadelphia Street Lane Closures | **Inference path only.** Current-state layer; expired permits are purged, so it holds no usable history before 2025. Keep the daily snapshot running so history accrues going forward |
| PASDA Bike Network / Street Centerlines | Determines which closures actually block a *cycling* route |

### Critical data gap

**There is no published historical archive of dock-level occupancy.** GBFS `station_status` is a live snapshot only. The label must therefore be reconstructed from trip data — see section 5.

---

## 5. Method and Pipelines

### The label problem

There is no published historical archive of dock-level occupancy — GBFS `station_status` is a live snapshot only. The label is therefore reconstructed from trip data by chaining each bike's trips on `bike_id`: where a bike's next trip starts at a different station from where its last trip ended, a van moved it, and a `reb_out` / `reb_in` pair is emitted.

This yields **two tiers of label**:

| Tier | Variable | Error |
|---|---|---|
| **Tier 1** | `net_flow(s,t)` = `arr − dep` | **Zero.** Counted directly from trip endpoints |
| Tier 2 | `O(s,t)`, `pct_full`, `is_empty` | Estimated — clamped cumulative ledger plus a solved initial condition |

**Train on Tier-1 `net_flow`.** At inference the true current occupancy is available from `station_status`, so the model only needs to predict the *change* — which is exactly what weather, time and closures drive — and reconstruction error never enters the learned weights. Tier 2 is retained as an input feature and as the basis of the `is_empty` classifier the web tool displays.

### Pipeline 1 — Pre-Training

| Phase | Does | Watch out for |
|---|---|---|
| **0 · Infrastructure** | Lake zones, catalog, `features.yaml`, **start the `station_status` poller** | The poller gates step 6.6 entirely and lost days are unrecoverable — deploy it first |
| **1 · Ingest** | Trip archives, station table, closures, geo layers, weather → `/raw` → `/bronze` | PGW is the only real closure history; the ArcGIS layer holds none before 2025 |
| **2 · Conform** | Normalise schema, localise timestamps, filter, dedupe → `silver.trips`; resolve PGW addresses to geometry | DST breaks any naive local-time join; `Virtual Station` rows corrupt mass balance |
| **3 · Labels** | Bike trajectories, rebalancing events, station-hour grid, `net_flow`, occupancy ledger | Step 3.1 must be a **single global pass** — partitioning by quarter injects a false rebalance every 3 months |
| **4 · Features** | Reproject to EPSG:2272, filter closures to bike-relevant, expand to hours, spatial join, weather, calendar, lags | Buffering in Web Mercator inflates distances ~1.29× at Philadelphia's latitude |
| **5 · Assembly** | Wide table, **chronological** split, fit encoders on train only, class weights | Never random-split — it leaks future weather and inflates metrics |
| **6 · Train** | Baselines, `net_flow` regressor, `is_empty` classifier, tune, evaluate, validate ledger, attribute | Evaluate on test exactly once; attribution is a deliverable, not a by-product |

### Pipeline 2 — Inference

Loads the artifact bundle from 6.8, reads live `station_status` for the integration constant `O(s,t₀)`, live Open-Meteo using the **identical** variable list pinned in `features.yaml`, and the current closure layer at the **same** buffer distance used in 4.4. Calendar features come from **shared code** with 4.6. It predicts `net_flow` forward, integrates from `O(s,t₀)`, and emits calibrated `P(empty)` per station per hour.

Live data is used **only** at inference. It is never a training input.

---

## 6. Quality Gates

### Ledger diagnostics (step 3.10)

| Check | Expected | Failure meaning |
|---|---|---|
| Global mass balance | `Σ reb_in == Σ reb_out` exactly, by construction | Pairing logic bug |
| Clamp violation rate | Low single-digit % per station | Timing error; concentrates at stations with heavy van activity |
| Out-of-system fleet count over time | Smooth few-% of fleet with a maintenance-shaped seasonal bump | A sawtooth means the 6h / 72h thresholds are mis-set |

### Modelling gates

- Chronological split only (5.2)
- Transformers fitted on train split only (5.3)
- Leakage audit passed for every feature (4.8)
- Full model beats the (station, hour, weekday) mean baseline (6.1)

---

## 7. Highest-Risk Items

1. **Step 0.5 — start the `station_status` poller today.** It gates step 6.6 entirely, and 6.6 is what makes the reconstruction defensible. Lost days are unrecoverable.
2. **Step 3.1 — the global `bike_id` pass.** The one stage where a partitioning mistake produces plausible-looking but wrong output that no downstream check will obviously catch.
3. **Step 4.1 — CRS handling.** Buffering in the wrong projection yields silently incorrect closure features, and the model will simply learn nothing from the closure arm.
4. **Step 2.6 — PGW address resolution.** PGW returns addresses as strings with no geometry, so the whole closure arm now depends on a street-name normalisation and centerline range join that does not exist yet. Validate it against the ~203 known-good coordinates in `LaneClosure_EUN_XY` before trusting the other ~108 K.

---

## 8. Repository Layout

```
.
├── README.md                           # This file — scope, sources, method summary
├── indego_capacity_data_sources.csv    # Source inventory with verified coverage
├── features.yaml                       # Shared feature config (step 0.4)
├── docs/
│   └── aws_architecture.drawio         # Deployed AWS architecture
├── collectors/                         # Continuous cron jobs (0.5, 1.3, 1.5)
├── pipelines/
│   ├── ingest/                         # Phase 1
│   ├── conform/                        # Phase 2
│   ├── labels/                         # Phase 3
│   ├── features/                       # Phase 4
│   ├── assembly/                       # Phase 5
│   └── training/                       # Phase 6
├── inference/                          # Pipeline 2
└── tests/
```

---

## 9. Open Decisions

| Decision | Status |
|---|---|
| Closure→station buffer distance (150 m starting point) | Open — treat as hyperparameter |
| Gap thresholds 6h / 72h / 30d | Open — tune against polled validation set |
| Training window start (2022 Q1 recommended) | Open |
| Demand vs. realised-flow modelling (censoring treatment) | Open — observed flow understates demand at saturated stations; options are censored regression (Tobit) or training only on non-saturated hours |
| PennDOT RCRS in or out of scope | **Closed — OUT.** It is live-only, so it has no training counterpart regardless of credential lead time, and the endpoint returned HTTP 500 on 2026-09-17. |
| Compute stack (Spark/YARN/HDFS vs. managed) | **Closed — managed.** EMR Serverless on S3 with the Glue Data Catalog; see §2. |
