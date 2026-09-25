#!/bin/bash
set -e

STACK_NAME="${1:-nx-neptune-demo}"
REGION="${2:-us-west-1}"

echo "Looking up stack ${STACK_NAME} in ${REGION}..."

# Get the staging bucket name from stack outputs
STAGING=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`StagingBucketName`].OutputValue' --output text)

# Disable it on the graph before deleting the stack (otherwise delete-stack fails).
# Look up the graph id from the stack outputs and clear protection; ignore errors if already disabled.
GRAPH_ID=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`GraphId`].OutputValue' --output text)
if [ -n "$GRAPH_ID" ] && [ "$GRAPH_ID" != "None" ]; then
  echo "Disabling deletion protection on graph ${GRAPH_ID}..."
  aws neptune-graph update-graph --graph-identifier "$GRAPH_ID" \
    --no-deletion-protection --region "$REGION" 2>/dev/null || true
fi

# Empty the versioned staging bucket
if [ -n "$STAGING" ] && [ "$STAGING" != "None" ]; then
  echo "Emptying s3://${STAGING}..."
  aws s3 rm "s3://${STAGING}" --recursive --region "$REGION" 2>/dev/null || true
  # Delete all object versions and delete markers
  aws s3api list-object-versions --bucket "$STAGING" --region "$REGION" --output json | \
    python3 -c "
import json,sys,subprocess
data=json.load(sys.stdin)
for key in ['Versions','DeleteMarkers']:
    for o in (data.get(key) or []):
        subprocess.run(['aws','s3api','delete-object','--bucket','$STAGING','--key',o['Key'],'--version-id',o['VersionId'],'--region','$REGION'],capture_output=True)
"
  echo "Bucket emptied."
fi

# Delete the stack
echo "Deleting stack ${STACK_NAME}..."
aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"
echo "Waiting for stack deletion..."
aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"
echo "Stack deleted."

# Clean up assets bucket
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ASSETS_BUCKET="nx-neptune-assets-${ACCOUNT_ID}-${REGION}"
echo "Cleaning up assets bucket s3://${ASSETS_BUCKET}..."
aws s3 rb "s3://${ASSETS_BUCKET}" --force --region "$REGION" 2>/dev/null || true

echo "Done."
