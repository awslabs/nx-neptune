# nx-neptune CloudFormation Template

This template creates a Neptune Analytics graph and a SageMaker notebook instance with `nx_neptune` pre-installed, environment variables pre-configured, and S3 import/export support.

## What it creates

- **Neptune Analytics graph** with configurable provisioned memory
- **SageMaker notebook instance** with `nx_neptune` installed and environment variables set
- **S3 staging bucket** with versioning + KMS encryption for import/export, datasets, Athena results, and general use
- **KMS key** for S3 bucket encryption
- **IAM role and policy** with Neptune, S3, KMS, Athena, Glue, and SageMaker permissions

## Prerequisites

- AWS CLI configured with valid credentials
- IAM permissions to create CloudFormation stacks, Neptune Analytics graphs, SageMaker notebooks, S3 buckets, KMS keys, and IAM roles

## Deploy

Build the wheel and zip the notebooks:

```bash
python -m pip wheel -w dist .
zip -r /tmp/notebooks.zip notebooks/
```

Upload both to an S3 bucket:

```bash
aws s3 cp dist/nx_neptune-*.whl s3://your-bucket/nx-neptune/
aws s3 cp /tmp/notebooks.zip s3://your-bucket/nx-neptune/
```

Deploy with the S3 prefix:

```bash
aws cloudformation deploy \
  --stack-name nx-neptune-demo \
  --template-file cloudformation-templates/nx-neptune-sagemaker.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides AssetsS3Prefix=s3://your-bucket/nx-neptune
```

If no `.whl` is found in the prefix, `nx_neptune` is installed from PyPI.

Alternatively, use the provided script which handles all of the above:

```bash
./cloudformation-templates/deploy.sh                        # defaults: nx-neptune-demo, us-west-1
./cloudformation-templates/deploy.sh my-stack us-east-1     # custom stack name and region
```

Set `BUILD_WHEEL=false` at the top of `deploy.sh` to skip wheel build and install from PyPI instead.

## Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| ApplicationId | Application id used to name all resources | `nx-neptune` |
| ProvisionedMemory | Number of m-NCUs for the graph (16, 32, 64) | `16` |
| PublicConnectivity | Enable public connectivity for the graph | `true` |
| NotebookInstanceType | SageMaker instance type | `ml.t3.medium` |
| AssetsS3Prefix | S3 prefix containing `notebooks.zip` and/or `.whl` | _(empty)_ |

## Environment variables

The following are automatically set on the notebook instance for all Jupyter kernels:

| Variable | Value |
|----------|-------|
| `NETWORKX_GRAPH_ID` | The created graph's ID |
| `AWS_REGION` | Stack region |
| `NETWORKX_S3_EXPORT_BUCKET_PATH` | `s3://<bucket>/export/` |
| `NETWORKX_S3_IMPORT_BUCKET_PATH` | `s3://<bucket>/import/` |
| `NETWORKX_STAGING_BUCKET` | `s3://<bucket>/staging` |

## Outputs

| Output | Description |
|--------|-------------|
| GraphId | Neptune Analytics graph ID |
| NotebookURL | SageMaker notebook URL |
| StagingBucketName | S3 bucket for import/export, datasets, Athena results, and general use |

```bash
aws cloudformation describe-stacks --stack-name nx-neptune-demo --query 'Stacks[0].Outputs'
```

## Teardown

The staging bucket has versioning enabled, so you must delete all object versions before stack deletion.

Use the provided script which handles bucket cleanup and stack deletion:

```bash
./cloudformation-templates/teardown.sh                      # defaults: nx-neptune-demo, us-west-1
./cloudformation-templates/teardown.sh my-stack us-east-1
```

Or manually:

```bash
# Empty the bucket (including versioned objects)
aws s3 rm s3://<bucket-name> --recursive

# Delete the stack
aws cloudformation delete-stack --stack-name nx-neptune-demo
aws cloudformation wait stack-delete-complete --stack-name nx-neptune-demo
```

## Notes

- The Neptune Analytics graph takes ~5–10 minutes to create
- Environment variables are set via `jupyter_notebook_config.py` and require a notebook stop/start to update
- On every notebook stop/start, `OnStart` re-installs `nx_neptune` (from the `.whl` if provided, otherwise PyPI)
