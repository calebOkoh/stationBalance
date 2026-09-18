# Infrastructure

Four Terraform layers, applied in order. Each is a separate root module with
its own local state under `infra/states/`, and each is driven by a
`deploy-<layer>.sh` script that does `init` then `apply` with the right
working directory.

```
./deploy-all.sh          # everything, in order
./deploy-lake.sh         # 1. S3 zones, Glue catalog, Athena, budget
./deploy-ingestion.sh    # 2. collectors + schedules   <- starts collecting
./deploy-pipeline.sh     # 3. EMR Serverless, SageMaker role, model registry
./deploy-serving.sh      # 4. DynamoDB, Pipeline 2 Lambdas, HTTP API, CloudFront
./destroy-all.sh         # reverse order, with a typed confirmation
```

`lake` must go first: the other three resolve its buckets by name through a
`data "aws_s3_bucket"` lookup rather than reading its state, so the layers stay
decoupled but the ordering is real. The remaining three are independent of each
other.

## What running `deploy-all.sh` actually gets you

Ingestion starts. The `station_status` poller begins writing to
`/raw/station_status/` within five minutes of apply and does not stop — that
data has no published archive and cannot be backfilled, which is why it is the
one thing that starts on its own.

It does **not** get you a trained model. Pipeline 1 is attended by design:
`.claude/decisions.md` cut Step Functions because every run is attended and
"numbered scripts give the same ordering with better debuggability — you can
stop halfway and inspect." The phases are driven by `scripts/`, in order, and
`scripts/40_publish_model.sh` is what turns Pipeline 2 on.

## Tagging

`service = station-balance` is applied to every taggable resource through the
provider's `default_tags`, so nothing has to be tagged individually and nothing
can be missed.

`step` is set per resource, because it varies. It names the resource's role in
the ML pipeline, so cost and inventory can be sliced by phase:

| `step` | Covers |
|---|---|
| `ingestion` | raw bucket, both collector Lambdas, their roles, the scheduler role |
| `catalog` | Glue database, the published `features.json` / `features.yaml` |
| `processing` | EMR Serverless application and job role, its log group |
| `assembly` | gold bucket |
| `training` | SageMaker execution role, model package group |
| `quality` | Athena workgroup |
| `inference` | inference Lambda and its role |
| `serving` | DynamoDB, API Gateway, CloudFront, status refresher, API Lambda |
| `observability` | the monthly budget |

Some resource types carry no tags at all — `aws_iam_role_policy`,
`aws_scheduler_schedule`, `aws_glue_catalog_table`, the S3 sub-resource
configurations and `aws_cloudfront_cache_policy` are not taggable in the AWS
provider. They inherit their identity from the resource they attach to.

## Cost

Steady state is the collectors and storage: roughly **$1–3/month**. Nothing in
these layers runs continuously except three schedules whose invocation counts
sit inside the EventBridge and Lambda free tiers.

The choices that keep it there, all of which are load-bearing:

- **No NAT Gateway.** Every Lambda is outside a VPC. A NAT Gateway is $32/mo,
  an order of magnitude above everything else combined.
- **No pre-initialised EMR capacity.** `preInitializedCapacity` is absent
  (therefore zero) and the application auto-stops after 15 idle minutes. A
  pipeline run is ~$1–2; idle is $0.
- **No SageMaker endpoint.** The model is off the request path — inference is
  a Lambda writing a precomputed cube. A warm endpoint would be the second
  largest line on the bill.
- **No managed MLflow tracking server** (~$460/mo). The SageMaker model package
  group is the registry.
- **DynamoDB on-demand**, ~12 K writes/hour against a provisioned floor that
  would never be approached.
- **Athena is capped** at 10 GB scanned per query. The whole lake is 3–5 GB, so
  anything above that is a missing partition predicate, not a real result.
- **S3 lifecycle**: Intelligent-Tiering on `/raw` (the poller grows without
  bound), and expiry on Athena results, EMR logs, and non-current versions of
  derived data.

A monthly budget filtered on `service=station-balance` alarms at 80% forecast
and 100% actual. Set `budget_alert_email` or the notifications are skipped.

## Variables worth knowing

| Variable | Default | Why you would change it |
|---|---|---|
| `aws_profile` | `coa-dev` | Must be able to create IAM roles **and attach policies** |
| `aws_region` | `us-east-1` | Everything is single-region; there is no cross-region path |
| `enable_collectors` | `true` | Ingestion layer. Leave on — the poller's data is unrecoverable |
| `enable_inference` | `false` | Serving layer. Turn on after the first model bundle exists |
| `budget_alert_email` | `""` | Empty creates the budget but no notification |
| `raw_bucket_name` / `gold_bucket_name` | `null` | Point a layer at a lake named differently |

## State

Local, under `infra/states/<layer>/`, gitignored — the same arrangement as
`pl-predictions-wtb`. Paths in the `backend "local"` block are relative to the
working directory, which is why every layer must be run through its deploy
script (or `terraform -chdir=infra/<layer>`) and not from inside the directory.
