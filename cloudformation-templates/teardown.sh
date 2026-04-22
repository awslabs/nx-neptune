#!/bin/bash
set -e

STACK_NAME="${1:-nx-neptune-demo}"
REGION="${2:-us-west-1}"

# Get the data bucket name from stack outputs
BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`S3BucketName`].OutputValue' --output text 2>/dev/null)

# Empty the versioned data bucket
if [ -n "$BUCKET" ] && [ "$BUCKET" != "None" ]; then
  echo "Emptying s3://${BUCKET}..."
  aws s3 rm "s3://${BUCKET}" --recursive --region "$REGION" 2>/dev/null || true
  aws s3api list-object-versions --bucket "$BUCKET" --region "$REGION" --output json 2>/dev/null | \
    python3 -c "
import json,sys,subprocess
data=json.load(sys.stdin)
for key in ['Versions','DeleteMarkers']:
    for o in (data.get(key) or []):
        subprocess.run(['aws','s3api','delete-object','--bucket','$BUCKET','--key',o['Key'],'--version-id',o['VersionId'],'--region','$REGION'],capture_output=True)
" 2>/dev/null
fi

# Delete the stack
echo "Deleting stack ${STACK_NAME}..."
aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"
aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"

# Clean up assets bucket
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ASSETS_BUCKET="nx-neptune-assets-${ACCOUNT_ID}-${REGION}"
echo "Cleaning up assets bucket s3://${ASSETS_BUCKET}..."
aws s3 rb "s3://${ASSETS_BUCKET}" --force --region "$REGION" 2>/dev/null || true

echo "Done."
