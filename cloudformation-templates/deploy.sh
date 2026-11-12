#!/bin/bash
set -e

STACK_NAME="${1:-nx-neptune-demo}"
REGION="${2:-us-west-1}"

if [ ${#STACK_NAME} -gt 16 ]; then
  echo "Error: STACK_NAME '${STACK_NAME}' exceeds 16 characters (ApplicationId limit)." >&2
  exit 1
fi

# Must match the template's ApplicationId AllowedPattern ([a-z][a-z0-9-]*, 3-16 chars).
# Validate here to fail fast, rather than surfacing a cryptic CloudFormation rejection after the work is done.
if ! printf '%s' "$STACK_NAME" | grep -Eq '^[a-z][a-z0-9-]{2,15}$'; then
  echo "Error: STACK_NAME '${STACK_NAME}' is invalid. Use 3-16 chars: lowercase letter first, then lowercase letters, digits, or hyphens." >&2
  exit 1
fi
BUILD_WHEEL="${3:-false}"
# Secure by default: private graph, deletion-protected. Override for a
# convenient demo, e.g. PUBLIC_CONNECTIVITY=true DELETION_PROTECTION=false ./deploy.sh
PUBLIC_CONNECTIVITY="${PUBLIC_CONNECTIVITY:-false}"
DELETION_PROTECTION="${DELETION_PROTECTION:-true}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"

echo "Cleaning stale build directory..."
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

# The bucket name is predictable, so refuse to use it unless this account owns it.
if ! HEAD_ERR=$(aws s3api head-bucket --bucket "$ASSETS_BUCKET" --expected-bucket-owner "$ACCOUNT_ID" --region "$REGION" 2>&1 >/dev/null); then
  if printf '%s' "$HEAD_ERR" | grep -q '(404)'; then
    aws s3 mb "s3://${ASSETS_BUCKET}" --region "$REGION"
    aws s3api head-bucket --bucket "$ASSETS_BUCKET" --expected-bucket-owner "$ACCOUNT_ID" --region "$REGION" >/dev/null
  else
    echo "Error: s3://${ASSETS_BUCKET} exists but is not owned by account ${ACCOUNT_ID}, or is not accessible. Refusing to upload." >&2
    echo "$HEAD_ERR" >&2
    exit 1
  fi
fi

upload_asset() {
  aws s3api put-object --bucket "$ASSETS_BUCKET" --key "$(basename "$1")" --body "$1" \
    --expected-bucket-owner "$ACCOUNT_ID" --region "$REGION" >/dev/null
}

echo "Uploading assets to s3://${ASSETS_BUCKET}/..."
if [ "$BUILD_WHEEL" = true ]; then
  for whl in "$BUILD_DIR"/nx_neptune-*.whl; do
    upload_asset "$whl"
  done
fi
upload_asset "$BUILD_DIR/notebooks.zip"

echo "Cleaning up build artifacts..."
rm -rf "$BUILD_DIR"

echo "Deploying stack ${STACK_NAME}..."
aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$SCRIPT_DIR/nx-neptune-sagemaker.json" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "$REGION" \
  --parameter-overrides "ApplicationId=${STACK_NAME}" "AssetsS3Prefix=s3://${ASSETS_BUCKET}" "CustomNotebooks=true" "PublicConnectivity=${PUBLIC_CONNECTIVITY}" "DeletionProtection=${DELETION_PROTECTION}" "GlueDatabaseScope=${STACK_NAME}"

echo ""
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" --query 'Stacks[0].Outputs' --output table
