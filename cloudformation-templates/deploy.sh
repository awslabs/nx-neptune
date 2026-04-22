#!/bin/bash
set -e

STACK_NAME="${1:-nx-neptune-demo}"
REGION="${2:-us-west-1}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Build wheel
echo "Building wheel..."
python -m pip wheel -w /tmp/nx-dist "$REPO_DIR" -q

# Zip notebooks
echo "Zipping notebooks..."
cd "$REPO_DIR"
zip -r /tmp/notebooks.zip notebooks/ -q

# Create assets bucket
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ASSETS_BUCKET="nx-neptune-assets-${ACCOUNT_ID}-${REGION}"
aws s3 mb "s3://${ASSETS_BUCKET}" --region "$REGION" 2>/dev/null || true

# Upload assets
echo "Uploading assets to s3://${ASSETS_BUCKET}/..."
aws s3 cp /tmp/nx-dist/nx_neptune-*.whl "s3://${ASSETS_BUCKET}/" --region "$REGION"
aws s3 cp /tmp/notebooks.zip "s3://${ASSETS_BUCKET}/" --region "$REGION"

# Deploy stack
echo "Deploying stack ${STACK_NAME}..."
aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$SCRIPT_DIR/nx-neptune-sagemaker.json" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "$REGION" \
  --parameter-overrides "AssetsS3Prefix=s3://${ASSETS_BUCKET}"

echo ""
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" --query 'Stacks[0].Outputs' --output table
