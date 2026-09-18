# Infrastructure

Three Terraform layers, applied in order. Each is a separate root module with
its own local state under `infra/states/`, driven by a `deploy-<layer>.sh`
script that does `init` then `apply` from the right working directory.

```
./deploy-all.sh          # everything, in order
./deploy-storage.sh      # 1. two S3 buckets, Glue catalog, Athena, budget
./deploy-ingestion.sh    # 2. the ingest Lambda
./deploy-pipeline.sh     # 3. EMR Serverless, SageMaker role, model registry
./destroy-all.sh         # reverse order, with a typed confirmation
```

`storage` must go first: the other two resolve its buckets by name through a
`data "aws_s3_bucket"` lookup rather than reading its state, so the layers stay
decoupled but the ordering is real.

## Nothing runs on its own

There is no EventBridge rule, no Step Functions state machine, and no scheduled
anything in this project. Every input is a published archive downloaded once,
and every phase is started by hand from `scripts/`.

That is not a simplification — it follows from the scope. This repository
builds a **pre-trained model** from historical data. Anything that reads a live
feed belongs to the inference architecture in
`docs/live_inference.drawio`, which is a design deliverable and is not built.
`scripts/00_preflight.sh` asserts the schedule count is zero for exactly this
reason.

## Storage layout

Two buckets. Plain prefix names, in the order data moves through them:

**`station-balance-data-<account>`**

| Prefix | Holds |
|---|---|
| `raw/` | Exactly what was downloaded, byte for byte. Versioned, never rewritten. |
| `parsed/` | The same data as Parquet. No cleaning, no filtering. |
| `clean/` | Conformed, filtered, deduplicated, and labelled. |

**`station-balance-model-<account>`**

| Prefix | Holds |
|---|---|
| `training/` | The wide modelling table and the frozen train/val/test splits. |
| `models/` | Exported model bundles, plus `models/current/`. |
| `code/` | Spark job source, `features.json`, the training tarball. |
| `athena-results/`, `emr-logs/` | Query output and driver logs. Both expire. |

Everything is reproducible from `raw/`, which is why only that prefix is
treated as precious.

## Tagging

`service = station-balance` is applied to every taggable resource through the
provider's `default_tags`, so nothing has to be tagged individually and nothing
can be missed.

`step` is set per resource and names its role in the pipeline, so cost and
inventory can be sliced by phase:

| `step` | Covers |
|---|---|
| `ingestion` | data bucket, the ingest Lambda and its role |
| `catalog` | Glue database, the published `features.json` / `features.yaml` |
| `processing` | EMR Serverless application and job role, its log group |
| `assembly` | model bucket |
| `training` | SageMaker execution role, model package group |
| `quality` | Athena workgroup |
| `observability` | the monthly budget |

Some resource types carry no tags at all — `aws_iam_role_policy`,
`aws_glue_catalog_table` and the S3 sub-resource configurations are not
taggable in the AWS provider. They inherit their identity from the resource
they attach to.

## Cost

At rest this is **S3 storage and nothing else** — a few GB, so well under
$1/month. There is no always-on compute anywhere in the account.

Running the pipeline end to end costs roughly **$2–4**: EMR Serverless at
~$1–2 for the batch phases, one SageMaker `ml.m5.4xlarge` training job at
~$0.25, and Lambda invocations inside the free tier.

The choices that keep it there, all load-bearing:

- **No NAT Gateway.** The Lambda is outside a VPC. A NAT Gateway is $32/mo, an
  order of magnitude above everything else combined.
- **No pre-initialised EMR capacity.** `preInitializedCapacity` is absent
  (therefore zero) and the application auto-stops after 15 idle minutes.
- **No SageMaker endpoint.** Nothing is served, so nothing is warm.
- **No managed MLflow tracking server** (~$460/mo). The SageMaker model package
  group is the registry.
- **Athena capped** at 10 GB scanned per query. The whole dataset is 3–5 GB, so
  anything above that is a missing partition predicate, not a real result.
- **S3 lifecycle**: expiry on Athena results, EMR logs, and non-current
  versions of anything derived.

A monthly budget filtered on `service=station-balance` alarms at 80% forecast
and 100% actual. Set `budget_alert_email` or the notifications are skipped.

## Variables worth knowing

| Variable | Default | Why you would change it |
|---|---|---|
| `aws_profile` | `coa-dev` | Must be able to create IAM roles **and attach policies** |
| `aws_region` | `us-east-1` | Single-region; there is no cross-region path |
| `budget_alert_email` | `""` | Empty creates the budget but no notification |
| `data_bucket_name` / `model_bucket_name` | `null` | Point a layer at buckets named differently |

## State

Local, under `infra/states/<layer>/`, gitignored — the same arrangement as
`pl-predictions-wtb`. Paths in the `backend "local"` block are relative to the
working directory, which is why every layer must be run through its deploy
script (or `terraform -chdir=infra/<layer>`) and not from inside the directory.
