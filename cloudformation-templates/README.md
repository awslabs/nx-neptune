# nx-neptune CloudFormation Template

This template creates a Neptune Analytics graph and a SageMaker notebook instance with `nx_neptune` pre-installed, environment variables pre-configured, and S3 import/export support.

## What it creates

- **Neptune Analytics graph** with configurable provisioned memory
- **SageMaker notebook instance** with `nx_neptune` installed and environment variables set
- **S3 staging bucket** with versioning + KMS encryption for import/export, datasets, Athena results, and general use
- **KMS key** for S3 bucket encryption
- **IAM role and policy** with Neptune, S3, KMS, Athena, Glue, and SageMaker permissions

### Notebook environment variables

The following are automatically set on the notebook instance for all Jupyter kernels:

| Variable | Value |
|----------|-------|
| `NETWORKX_GRAPH_ID` | The created graph's ID |
| `AWS_REGION` | Stack region |
| `NETWORKX_S3_EXPORT_BUCKET_PATH` | `s3://<bucket>/export/` |
| `NETWORKX_S3_IMPORT_BUCKET_PATH` | `s3://<bucket>/import/` |
| `NETWORKX_STAGING_BUCKET` | `s3://<bucket>/staging` |

## Prerequisites

- AWS CLI configured with valid credentials
- IAM permissions to create CloudFormation stacks, Neptune Analytics graphs, SageMaker notebooks, S3 buckets, KMS keys, and IAM roles

## Deploy with script

The provided script handles building, uploading assets, and deploying the stack:

```bash
./cloudformation-templates/deploy.sh                                # defaults: nx-neptune-demo, us-west-1, no wheel build
./cloudformation-templates/deploy.sh nx-neptune-demo us-east-1      # custom stack name and region
./cloudformation-templates/deploy.sh nx-neptune-demo us-east-1 true # build and deploy a local wheel
```

The script accepts three positional arguments:

| Argument | Position | Default | Description |
|----------|----------|---------|-------------|
| `STACK_NAME` | 1st | `nx-neptune-demo` | CloudFormation stack name (also used as `ApplicationId`, max 16 characters) |
| `REGION` | 2nd | `us-west-1` | AWS region |
| `BUILD_WHEEL` | 3rd | `false` | Set to `true` to build and deploy a local wheel; `false` installs from PyPI |

Notes: 
 - The stack name is passed as the `ApplicationId` parameter, which names all created resources. 

## Deploy with `aws cloudformation` CLI

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

### Parameters

These are the `--parameter-overrides` accepted by the CloudFormation template:

| Parameter | Description | Default |
|-----------|-------------|---------|
| ApplicationId | Application id used to name all resources (max 16 characters) | `nx-neptune` |
| ProvisionedMemory | Number of m-NCUs for the graph (16, 32, 64) | `16` |
| PublicConnectivity | Enable public connectivity for the graph | `true` |
| NotebookInstanceType | SageMaker instance type | `ml.t3.medium` |
| AssetsS3Prefix | S3 prefix containing `notebooks.zip` and optionally a `.whl` | _(required)_ |

### Example with all parameters

```bash
aws cloudformation deploy \
  --stack-name nx-neptune-demo \
  --template-file cloudformation-templates/nx-neptune-sagemaker.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ApplicationId=my-custom-name \
    ProvisionedMemory=32 \
    PublicConnectivity=false \
    NotebookInstanceType=ml.t3.large \
    AssetsS3Prefix=s3://your-bucket/nx-neptune
```

## Outputs

| Output | Description |
|--------|-------------|
| GraphId | Neptune Analytics graph ID |
| NotebookURL | SageMaker notebook URL |
| StagingBucketName | S3 bucket for import/export, datasets, Athena results, and general use |

```bash
aws cloudformation describe-stacks --stack-name nx-neptune-demo --query 'Stacks[0].Outputs'
```

## Stack Teardown

The staging bucket has versioning enabled, so you must delete all object versions before stack deletion.

Use the provided script which handles bucket cleanup and stack deletion:

```bash
./cloudformation-templates/teardown.sh                      # defaults: nx-neptune-demo, us-west-1
./cloudformation-templates/teardown.sh nx-neptune-demo us-east-1
```

Or manually:

```bash
# Empty the bucket (including versioned objects)
aws s3 rm s3://<your-bucket> --recursive

# Delete the stack
aws cloudformation delete-stack --stack-name nx-neptune-demo
aws cloudformation wait stack-delete-complete --stack-name nx-neptune-demo
```

## Notes

- The Neptune Analytics graph takes ~5–10 minutes to create
- Environment variables are set via `jupyter_notebook_config.py` and require a notebook stop/start to update
- On every notebook stop/start, `OnStart` re-installs `nx_neptune` (from the `.whl` if provided, otherwise PyPI)
