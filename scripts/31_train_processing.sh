#!/usr/bin/env bash
# Phase 6 -- the SAME training run as 30_train.sh, submitted as a SageMaker
# PROCESSING job instead of a TRAINING job.
#
# Why this file exists: this account's quota for every `ml.* for training job
# usage` is 0, in every region, and raising it needs an AWS Support case that
# had not cleared. The quota for `ml.t3.xlarge for processing job usage` is 2.
# Processing and Training are the same service, the same execution role, and
# the same container -- the quota is what differs, so this runs the identical
# entrypoint on the capacity the account actually has. See .claude/decisions.md.
#
# train.py is NOT modified. It resolves its channels from SM_CHANNEL_* and
# SM_MODEL_DIR, so pointing those at the processing container's mount paths is
# the whole adaptation.
#
# The output is byte-identical in SHAPE to a training job's: the entrypoint
# tars the bundle to model.tar.gz under the output channel, so it lands at
# s3://$MODEL_BUCKET/models/<job>/output/model.tar.gz and 40_publish_model.sh
# consumes it unchanged.
#
# Usage:  ./31_train_processing.sh [job-name-suffix]
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

MODEL_BUCKET="$(tf_output storage model_bucket)"
SM_ROLE="$(tf_output pipeline sagemaker_role_arn)"

# t3.xlarge: 4 vCPU / 16 GiB. The splits peak around 4-5 GiB in pandas plus
# LightGBM's binned Dataset, so memory is comfortable and the burstable CPU is
# the binding constraint -- expect hours, not the ~20 min an m5.4xlarge takes.
INSTANCE="${PROCESSING_INSTANCE:-ml.t3.xlarge}"

SUFFIX="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
JOB_NAME="station-balance-$SUFFIX"
CODE_PREFIX="s3://$MODEL_BUCKET/code/processing"
IMAGE="683313688378.dkr.ecr.$AWS_REGION.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3"

echo "==> staging the entrypoint"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
cp "$PROJECT_ROOT/pipelines/training/train.py" "$STAGE/"
cp "$PROJECT_ROOT/pipelines/training/requirements.txt" "$STAGE/"
# features.json rather than the YAML, for the same reason 30_train.sh does it:
# the container has no PyYAML guarantee, and this is the rendered contract the
# Spark phases already read.
aws s3 cp "s3://$MODEL_BUCKET/code/features.json" "$STAGE/features.json"

# A processing job has no script mode, so there is no pip step for free. The
# entrypoint does what the training toolkit would have done: install the pins,
# run the module, then package the bundle.
cat > "$STAGE/run.sh" <<'RUN'
#!/bin/bash
set -euo pipefail
cd /opt/ml/processing/code

echo "=== installing pinned dependencies ==="
# Into an ISOLATED tree, not over the image's environment.
#
# This image is a conda env at /miniconda3 (Python 3.9) whose scipy 1.8.0 and
# numpy 1.24.1 are pinned by `sagemaker-sklearn-container 2.0`. Installing the
# pins on top of it half-replaces those conda packages and leaves scipy with a
# stale compiled `dfitpack`, so `scipy.interpolate` raises
#
#   TypeError ... _fitpack_impl.py, line 103, in <module>
#
# which `sklearn.metrics` imports transitively -- the failure lands on an
# unrelated import and looks nothing like its cause. Observed on job
# station-balance-20260918T185407Z.
#
# --target puts the whole pinned closure in one directory, and PYTHONPATH
# precedes site-packages on sys.path, so every import resolves to a consistent
# set and the conda env is left untouched rather than half-upgraded.
#
# --no-cache-dir: pip's cache is pure waste in a process that runs exactly once.
DEPS=/opt/ml/processing/deps
pip install --no-cache-dir --target "$DEPS" -r requirements.txt
export PYTHONPATH="$DEPS"

echo "=== python / library versions ==="
# Prints the resolved FILE for each, not just the version: the whole failure
# mode above is an import resolving somewhere other than where it was installed.
python -c "
import sys, lightgbm, pandas, numpy, sklearn, scipy, shap
print('python', sys.version.split()[0])
for m in (lightgbm, pandas, numpy, sklearn, scipy, shap):
    print(f'  {m.__name__:<12} {m.__version__:<10} {m.__file__}')
"

mkdir -p "$SM_MODEL_DIR" /opt/ml/processing/output

echo "=== phase 6 ==="
python train.py

# Package exactly as a training job would, so the publish step cannot tell the
# difference. Contents, not the directory: tar -C then '.' keeps the members at
# the archive root, which is where 40_publish_model.sh looks for them.
echo "=== packaging the bundle ==="
tar -czf /opt/ml/processing/output/model.tar.gz -C "$SM_MODEL_DIR" .
tar -tzf /opt/ml/processing/output/model.tar.gz
RUN

aws s3 cp "$STAGE/train.py"         "$CODE_PREFIX/train.py"         --only-show-errors
aws s3 cp "$STAGE/requirements.txt" "$CODE_PREFIX/requirements.txt" --only-show-errors
aws s3 cp "$STAGE/features.json"    "$CODE_PREFIX/features.json"    --only-show-errors
aws s3 cp "$STAGE/run.sh"           "$CODE_PREFIX/run.sh"           --only-show-errors

echo "==> starting $JOB_NAME on $INSTANCE (processing job)"
aws sagemaker create-processing-job \
  --processing-job-name "$JOB_NAME" \
  --role-arn "$SM_ROLE" \
  --app-specification "{
    \"ImageUri\": \"$IMAGE\",
    \"ContainerEntrypoint\": [\"/bin/bash\", \"/opt/ml/processing/code/run.sh\"]
  }" \
  --environment "{
    \"SM_CHANNEL_TRAIN\": \"/opt/ml/processing/train\",
    \"SM_CHANNEL_VALIDATION\": \"/opt/ml/processing/validation\",
    \"SM_CHANNEL_TEST\": \"/opt/ml/processing/test\",
    \"SM_MODEL_DIR\": \"/opt/ml/processing/model\"
  }" \
  --processing-inputs "[
    {\"InputName\": \"code\",       \"S3Input\": {\"S3Uri\": \"$CODE_PREFIX/\",                        \"LocalPath\": \"/opt/ml/processing/code\",       \"S3DataType\": \"S3Prefix\", \"S3InputMode\": \"File\", \"S3DataDistributionType\": \"FullyReplicated\"}},
    {\"InputName\": \"train\",      \"S3Input\": {\"S3Uri\": \"s3://$MODEL_BUCKET/training/train/\",    \"LocalPath\": \"/opt/ml/processing/train\",      \"S3DataType\": \"S3Prefix\", \"S3InputMode\": \"File\", \"S3DataDistributionType\": \"FullyReplicated\"}},
    {\"InputName\": \"validation\", \"S3Input\": {\"S3Uri\": \"s3://$MODEL_BUCKET/training/val/\",      \"LocalPath\": \"/opt/ml/processing/validation\", \"S3DataType\": \"S3Prefix\", \"S3InputMode\": \"File\", \"S3DataDistributionType\": \"FullyReplicated\"}},
    {\"InputName\": \"test\",       \"S3Input\": {\"S3Uri\": \"s3://$MODEL_BUCKET/training/test/\",     \"LocalPath\": \"/opt/ml/processing/test\",       \"S3DataType\": \"S3Prefix\", \"S3InputMode\": \"File\", \"S3DataDistributionType\": \"FullyReplicated\"}}
  ]" \
  --processing-output-config "{
    \"Outputs\": [
      {\"OutputName\": \"bundle\", \"S3Output\": {\"S3Uri\": \"s3://$MODEL_BUCKET/models/$JOB_NAME/output/\", \"LocalPath\": \"/opt/ml/processing/output\", \"S3UploadMode\": \"EndOfJob\"}}
    ]
  }" \
  --processing-resources "{
    \"ClusterConfig\": {\"InstanceType\": \"$INSTANCE\", \"InstanceCount\": 1, \"VolumeSizeInGB\": 50}
  }" \
  --stopping-condition "{\"MaxRuntimeInSeconds\": 21600}" \
  --tags Key=service,Value=station-balance Key=step,Value=training \
  --output text --query 'ProcessingJobArn'

echo "==> waiting (burstable CPU -- budget 1.5-3 h)"
# Not `aws sagemaker wait`: its waiter gives up after 60 attempts at 60s, so a
# run longer than an hour exits non-zero with the job still healthy. On a
# throttled t3 that is the EXPECTED case, not the edge case.
STATUS=InProgress
while [[ "$STATUS" == "InProgress" ]]; do
  sleep 60
  STATUS="$(aws sagemaker describe-processing-job --processing-job-name "$JOB_NAME" \
    --query 'ProcessingJobStatus' --output text)"
  echo "    [$(date -u +%H:%M:%SZ)] $STATUS"
done
echo "    $STATUS"

if [[ "$STATUS" != "Completed" ]]; then
  aws sagemaker describe-processing-job --processing-job-name "$JOB_NAME" \
    --query 'FailureReason' --output text
  exit 1
fi

echo
echo "Artifacts: s3://$MODEL_BUCKET/models/$JOB_NAME/output/model.tar.gz"
echo "Publish them with:  ./40_publish_model.sh $JOB_NAME"
