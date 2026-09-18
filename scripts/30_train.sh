#!/usr/bin/env bash
# Phase 6 -- launches the SageMaker training job.
#
# One ml.m5.4xlarge, ~$0.25 per run (README section 2). The job trains both
# targets: the net_flow regressor (Tier-1, the real target) and the is_empty
# classifier the web tool displays, then runs the SHAP attribution that is the
# research deliverable.
#
# Usage:  ./30_train.sh [job-name-suffix]
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

MODEL_BUCKET="$(tf_output storage model_bucket)"
SM_ROLE="$(tf_output pipeline sagemaker_role_arn)"
INSTANCE="$(tf_output pipeline training_instance_type)"

SUFFIX="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
JOB_NAME="station-balance-$SUFFIX"
SOURCE_URI="s3://$MODEL_BUCKET/code/training-source.tar.gz"

echo "==> packaging the training source"
STAGE="$(mktemp -d)"
cp "$PROJECT_ROOT/pipelines/training/train.py" "$STAGE/"
cp "$PROJECT_ROOT/pipelines/training/requirements.txt" "$STAGE/"
# features.json rather than the YAML: the training container has no PyYAML
# guarantee either, and this is the same rendered contract the Spark phases read.
aws s3 cp "s3://$MODEL_BUCKET/code/features.json" "$STAGE/features.json"
tar -czf "$STAGE/training-source.tar.gz" -C "$STAGE" train.py requirements.txt features.json
aws s3 cp "$STAGE/training-source.tar.gz" "$SOURCE_URI"
rm -rf "$STAGE"

echo "==> starting $JOB_NAME on $INSTANCE"
aws sagemaker create-training-job \
  --training-job-name "$JOB_NAME" \
  --role-arn "$SM_ROLE" \
  --algorithm-specification "{
    \"TrainingImage\": \"683313688378.dkr.ecr.$AWS_REGION.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3\",
    \"TrainingInputMode\": \"File\"
  }" \
  --hyper-parameters "{
    \"sagemaker_program\": \"\\\"train.py\\\"\",
    \"sagemaker_submit_directory\": \"\\\"$SOURCE_URI\\\"\"
  }" \
  --input-data-config "[
    {\"ChannelName\": \"train\", \"DataSource\": {\"S3DataSource\": {\"S3DataType\": \"S3Prefix\", \"S3Uri\": \"s3://$MODEL_BUCKET/training/train/\", \"S3DataDistributionType\": \"FullyReplicated\"}}},
    {\"ChannelName\": \"validation\", \"DataSource\": {\"S3DataSource\": {\"S3DataType\": \"S3Prefix\", \"S3Uri\": \"s3://$MODEL_BUCKET/training/val/\", \"S3DataDistributionType\": \"FullyReplicated\"}}},
    {\"ChannelName\": \"test\", \"DataSource\": {\"S3DataSource\": {\"S3DataType\": \"S3Prefix\", \"S3Uri\": \"s3://$MODEL_BUCKET/training/test/\", \"S3DataDistributionType\": \"FullyReplicated\"}}}
  ]" \
  --output-data-config "{\"S3OutputPath\": \"s3://$MODEL_BUCKET/models/\"}" \
  --resource-config "{\"InstanceType\": \"$INSTANCE\", \"InstanceCount\": 1, \"VolumeSizeInGB\": 50}" \
  --stopping-condition "{\"MaxRuntimeInSeconds\": 7200}" \
  --tags "Key=service,Value=station-balance Key=step,Value=training" \
  --output text --query 'TrainingJobArn'

echo "==> waiting"
aws sagemaker wait training-job-completed-or-stopped --training-job-name "$JOB_NAME"

STATUS="$(aws sagemaker describe-training-job --training-job-name "$JOB_NAME" \
  --query 'TrainingJobStatus' --output text)"
echo "    $STATUS"

if [[ "$STATUS" != "Completed" ]]; then
  aws sagemaker describe-training-job --training-job-name "$JOB_NAME" \
    --query 'FailureReason' --output text
  exit 1
fi

echo
echo "Artifacts: s3://$MODEL_BUCKET/models/$JOB_NAME/output/model.tar.gz"
echo "Publish them with:  ./40_publish_model.sh $JOB_NAME"
