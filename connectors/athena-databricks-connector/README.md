## Athena Databricks Connector

This connector enables Amazon Athena to query data stored in Databricks Unity Catalog using JDBC. It allows you to perform federated queries on Databricks tables directly from Athena.

## Connector Status: Preview

The Databricks Athena connector is currently in **preview** and available only as source code for building locally. This is not a production-ready release.

We welcome questions, suggestions, and contributions from the community.

## What is the Databricks Connector?

The Databricks Connector is a JDBC-based Athena federated query connector that enables querying data in Databricks Unity Catalog. It implements both metadata and record handling capabilities to:

1. Provide schema information about Databricks databases, tables, and columns
2. Read data from Databricks tables for query processing via JDBC
3. Authenticate using personal access tokens stored in AWS Secrets Manager

The connector consists of:

- **DatabricksMetadataHandler**: Handles metadata operations (list schemas, tables, get table definitions, partitions, and splits)
- **DatabricksRecordHandler**: Handles data reading operations from Databricks via JDBC
- **DatabricksCompositeHandler**: Combines both handlers into a single Lambda function

## Prerequisites

Before deploying this connector, ensure you have:

- [Proper permissions/policies to deploy/use Athena Federated Queries](https://docs.aws.amazon.com/athena/latest/ug/federated-query-iam-access.html)
- An S3 bucket for spilling large query results
- A Databricks workspace with Unity Catalog enabled
- A Databricks personal access token stored in AWS Secrets Manager
- Athena workgroup configured to use Athena Engine Version 3

## How To Deploy

### Build the Connector

```bash
mvn clean package -DskipTests
```

### Deploy Using SAM CLI

```bash
sam build -t athena-databricks-connector.yaml && sam deploy --guided -t athena-databricks-connector.yaml
```

### CloudFormation Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| AthenaCatalogName | Lambda function name (must match pattern: `^[a-z0-9-_]{1,64}$`) | databricks |
| SpillBucket | S3 bucket for spilling data | Required |
| SpillPrefix | Prefix within SpillBucket | athena-spill |
| LambdaTimeout | Maximum Lambda invocation runtime (1-900 seconds) | 900 |
| LambdaMemory | Lambda memory in MB (128-3008) | 1024 |
| SecretNameOrPrefix | Name or prefix of the Secrets Manager secret containing the Databricks personal access token | Required |
| DatabricksDefaultDatabase | Default Databricks Unity Catalog database (catalog.schema) | default |
| DisableSpillEncryption | Disable encryption for spilled data | false |

### Update Lambda Function

For subsequent code updates after initial deployment, build and push the Docker image manually:

```bash
mvn clean package -DskipTests && \
finch build -t databricks-connector . && \
finch tag databricks-connector:latest <account-id>.dkr.ecr.<region>.amazonaws.com/<repo-name>:latest && \
finch push <account-id>.dkr.ecr.<region>.amazonaws.com/<repo-name>:latest && \
aws lambda update-function-code \
  --function-name databricks \
  --image-uri <account-id>.dkr.ecr.<region>.amazonaws.com/<repo-name>:latest \
  --region <region>
```

**Note:** The image must be built for `linux/amd64` platform. On Apple Silicon Macs, Finch defaults to `arm64` which will cause `Runtime.InvalidEntrypoint` errors on Lambda.

## Secrets Manager Configuration

The connector retrieves Databricks connection credentials from AWS Secrets Manager. Create a secret with the prefix specified in `SecretNameOrPrefix` containing your Databricks connection details (host, token, port). The Lambda execution role is granted read-only access (`secretsmanager:GetSecretValue`) to secrets matching the specified prefix.

## Run Queries

Once deployed, query Databricks data through Athena:

```sql
-- List databases
SELECT * FROM "databricks"."information_schema"."schemata";

-- Query a table
SELECT * FROM "databricks"."default"."your_table" LIMIT 10;
```

You can run queries from the Athena console or the AWS CLI:

```bash
aws athena start-query-execution \
  --query-string 'SELECT * FROM "databricks"."default"."your_table" LIMIT 10' \
  --work-group primary \
  --region <region>
```

## Troubleshooting

- **Check Lambda Logs**: `aws logs tail /aws/lambda/databricks --follow --format short --region <region>`
- **Verify Permissions**: Ensure the Lambda execution role has access to Secrets Manager and the spill bucket

## Additional Resources

- [Athena Federated Query Documentation](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source.html)
- [AWS Athena Query Federation SDK](https://github.com/awslabs/aws-athena-query-federation)
- [Databricks JDBC Driver](https://docs.databricks.com/aws/en/integrations/jdbc-oss/)
