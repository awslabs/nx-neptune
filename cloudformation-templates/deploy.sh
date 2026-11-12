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
# Scope the name per stack (as well as account and region). The ownership checks
# below are what keep this safe; scoping just avoids a single shared name across
# stacks (with the default STACK_NAME the name is still predictable). STACK_NAME
# is capped at 16 chars above, keeping this within S3's 63-char bucket-name limit.
ASSETS_BUCKET="nx-neptune-assets-${STACK_NAME}-${ACCOUNT_ID}-${REGION}"

if aws s3api head-bucket --bucket "$ASSETS_BUCKET" --expected-bucket-owner "$ACCOUNT_ID" --region "$REGION" 2>/dev/null; then
  # Already exists in this account (e.g. a prior deploy). Confirm it is in the
  # expected region before reusing it; a name resolving to another region would
  # otherwise route uploads through redirects.
  LOC=$(aws s3api get-bucket-location --bucket "$ASSETS_BUCKET" --expected-bucket-owner "$ACCOUNT_ID" --query 'LocationConstraint' --output text)
  if [ "$LOC" = "None" ]; then LOC="us-east-1"; fi
  if [ "$LOC" != "$REGION" ]; then
    echo "Error: s3://${ASSETS_BUCKET} is in ${LOC}, not ${REGION}. Refusing to upload." >&2
    exit 1
  fi
else
  # Not owned by this account, or does not exist. Create it fresh: S3's global
  # namespace makes create-bucket fail closed if the name is already taken in
  # another account, so we never adopt a bucket we do not own.
  echo "Creating assets bucket s3://${ASSETS_BUCKET}..."
  create_args=(--bucket "$ASSETS_BUCKET" --region "$REGION")
  if [ "$REGION" != "us-east-1" ]; then
    create_args+=(--create-bucket-configuration "LocationConstraint=${REGION}")
  fi
  if ! CREATE_ERR=$(aws s3api create-bucket "${create_args[@]}" 2>&1); then
    if printf '%s' "$CREATE_ERR" | grep -q 'BucketAlreadyOwnedByYou'; then
      # This account owns the bucket, but the earlier head-bucket could not
      # confirm it (commonly a missing s3:ListBucket, or a transient error).
      echo "Error: s3://${ASSETS_BUCKET} already exists in this account but its ownership could not be confirmed. Check the caller's s3:ListBucket permission and retry. Refusing to upload." >&2
    else
      echo "Error: could not create s3://${ASSETS_BUCKET}. The name may already be taken in another account, or you may lack permission. Refusing to upload." >&2
    fi
    echo "$CREATE_ERR" >&2
    exit 1
  fi
  # Confirm we own what we just created before uploading to it. (In us-east-1,
  # create-bucket succeeds for a bucket we already own, so this is the only
  # ownership gate on that path.)
  if ! aws s3api head-bucket --bucket "$ASSETS_BUCKET" --expected-bucket-owner "$ACCOUNT_ID" --region "$REGION" >/dev/null 2>&1; then
    echo "Error: created s3://${ASSETS_BUCKET} but could not confirm ownership (check the caller's s3:ListBucket permission). Refusing to upload." >&2
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
