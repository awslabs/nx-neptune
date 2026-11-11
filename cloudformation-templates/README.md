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
| GlueDatabaseScope | **Required.** Glue database the notebook may create/update/drop Iceberg export tables in. Scopes `glue:CreateTable`/`glue:UpdateTable`/`glue:DeleteTable` and sets `NETWORKX_S3_TABLES_DATABASE`. | _(required)_ |
| PublicConnectivity | Enable public connectivity for the graph. `false` = private (VPC-only); the bundled notebook is **not** placed in the graph's VPC, so set `true` to run the demo today. | `false` |
| DeletionProtection | Protect the graph from deletion (`false` for ephemeral/demo graphs) | `true` |
| NotebookInstanceType | SageMaker instance type | `ml.t3.medium` |
| AssetsS3Prefix | S3 prefix containing `notebooks.zip` and optionally a `.whl` | _(required)_ |

> **Note on `PublicConnectivity`.** The `false` default is the secure posture, but
> this template doesn't place the notebook in the graph's VPC — so with `false` the
> notebook can't reach the graph. Set `PublicConnectivity=true` to run the demo today
> (a deliberate opt-in). A VPC-placed variant is planned, after which `false` becomes
> the working secure default.

### Example with all parameters

```bash
aws cloudformation deploy \
  --stack-name nx-neptune-demo \
  --template-file cloudformation-templates/nx-neptune-sagemaker.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ApplicationId=my-custom-name \
    ProvisionedMemory=32 \
    PublicConnectivity=true \
    NotebookInstanceType=ml.t3.large \
    AssetsS3Prefix=s3://your-bucket/nx-neptune
```

> The example sets `PublicConnectivity=true` because the bundled notebook is not
> placed in the graph's VPC yet (see the note above). Use `false` only once the
> client can reach the graph privately.

## Outputs

| Output | Description |
|--------|-------------|
| GraphId | Neptune Analytics graph ID |
| NotebookURL | SageMaker notebook URL |
| StagingBucketName | S3 bucket for import/export, datasets, Athena results, and general use |
| ImportRoleArn | Role Neptune Analytics assumes for S3 import and export. Its trust is unconditioned because the service does not satisfy `aws:SourceAccount` or `aws:SourceArn` |

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

### Neptune Analytics import role trust policy

Neptune Analytics assumes an IAM role to read S3 for imports and write it for exports. This
template provides a dedicated role for that, `ImportRole`, exported as the `ImportRoleArn`
output and set in the notebook as `NETWORKX_S3_IAM_ROLE_ARN`. It exists because of the
measurement below.

#### What was measured

On 2026-11-11, in `us-east-1`, against a 16 m-NCU graph, via `StartImportTask`, one throwaway
role per form, one run each:

| Trust condition on the `neptune-graph.amazonaws.com` principal | Result |
| --- | --- |
| `StringEquals aws:SourceAccount` | rejected |
| `ArnLike aws:SourceArn …:neptune-graph:<region>:<acct>:graph/*` | rejected |
| `ArnLike aws:SourceArn …:neptune-graph:<region>:<acct>:*` | rejected |
| both of the above together | rejected |
| no condition | accepted, import succeeded |

Every rejection returned the same message, which names S3 and not trust:

```
ValidationException: The S3 arn s3://<bucket>/<prefix>/ is inaccessible or your role
does not have access to it.
```

The accepted run used the same bucket, the same S3 read policy and the same IAM propagation
delay as the rejected ones, so the trust condition was the only variable that changed.

**What this does and does not establish.** It establishes that these four conditioned forms
were rejected and that an unconditioned one was accepted, under those conditions. It does not
establish *why*: the natural reading is that the service does not supply those context keys, but
this was one run per form, in one region, through one API, and the conditioned roles produced no
`AssumeRole` events in CloudTrail at all — which fits a rejection before any assume attempt just
as well as a condition that failed to match. Other operators and ARN shapes were not tried. If
you need certainty for your own threat model, re-run it (below) rather than relying on this.

#### Why `ImportRole` has no trust condition, and why that is contained

An `aws:SourceAccount` or `aws:SourceArn` condition is the usual confused-deputy protection for
a service principal, and `ImportRole` does not have one, because with one it cannot do its job.
The protection is replaced by making the role not worth confusing: its only permissions are
`s3:GetObject`, `s3:ListBucket` and `s3:PutObject` on this stack's staging bucket, and
`kms:Decrypt`/`GenerateDataKey`/`DescribeKey` on that bucket's key. No Neptune actions, no Glue,
no logs, no SageMaker. Anything that managed to have the service assume it could read and write
one bucket.

`NotebookRole` is deliberately **not** changed. It keeps its conditioned `neptune-graph`
statement, which means it still cannot be used for import — but it also holds Neptune
administration, Glue table writes and broader S3 access, and an unconditioned service trust on
a role that powerful is a much larger grant than the same trust on `ImportRole`.

Note the key-policy asymmetry: the staging bucket's KMS key names `NotebookRole` explicitly but
not `ImportRole`. `ImportRole` reaches the key through the key policy's `AllowRootAccount`
statement, which delegates to IAM. Naming it explicitly is not possible — the key policy would
depend on `ImportRole` while `ImportRole`'s policy already depends on the key, and CloudFormation
rejects that as circular. `NotebookRole` can be named only because its permissions live in the
separate `NeptuneAccessPolicy` rather than inline.

See also the import-role requirements in the Getting Started prerequisites, which document the
same unconditioned-trust requirement for roles you build yourself.

#### To re-measure

The test is skipped unless `NETWORKX_GRAPH_ID` and `NETWORKX_S3_EXPORT_BUCKET_PATH` are set. It
creates and deletes throwaway IAM roles prefixed `nxn-trusttest-`, starts real imports, and adds
data to the graph without resetting it, so point it at a disposable graph:

```bash
NETWORKX_GRAPH_ID=g-xxxxx NETWORKX_S3_EXPORT_BUCKET_PATH=s3://<bucket>/<prefix>/ \
  pytest integ_test/session_manager/test_import_role_trust.py -v -rA
```

It records a verdict per form and asserts nothing about which one passes, so a green run is not
a result — read the per-form summary it prints. The IAM propagation wait is
`NX_TRUST_TEST_IAM_WAIT_SECONDS` (default 20 seconds); raise it if a form is rejected
unexpectedly, since too short a wait looks the same as a refused trust.
