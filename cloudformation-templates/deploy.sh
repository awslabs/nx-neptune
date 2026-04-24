#!/bin/bash
set -e

STACK_NAME="${1:-nx-neptune-demo}"
REGION="${2:-us-west-1}"
BUILD_WHEEL=true
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

if [ "$BUILD_WHEEL" = true ]; then
  echo "Building wheel..."
  python -m pip wheel -w "$BUILD_DIR" "$REPO_DIR" -q
fi

echo "Zipping notebooks..."
cd "$REPO_DIR"
zip -r "$BUILD_DIR/notebooks.zip" notebooks/ -q

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ASSETS_BUCKET="nx-neptune-assets-${ACCOUNT_ID}-${REGION}"
aws s3 mb "s3://${ASSETS_BUCKET}" --region "$REGION" 2>/dev/null || true

echo "Uploading assets to s3://${ASSETS_BUCKET}/..."
if [ "$BUILD_WHEEL" = true ]; then
  aws s3 cp "$BUILD_DIR"/nx_neptune-*.whl "s3://${ASSETS_BUCKET}/" --region "$REGION"
fi
aws s3 cp "$BUILD_DIR/notebooks.zip" "s3://${ASSETS_BUCKET}/" --region "$REGION"

echo "Cleaning up build artifacts..."
rm -rf "$BUILD_DIR"

echo "Deploying stack ${STACK_NAME}..."
aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$SCRIPT_DIR/nx-neptune-sagemaker.json" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "$REGION" \
  --parameter-overrides "AssetsS3Prefix=s3://${ASSETS_BUCKET}"

echo ""
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" --query 'Stacks[0].Outputs' --output table
