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

From the repository root, initialize the submodule and build:

```bash
git submodule update --init
cd connectors
mvn clean package -DskipTests
```

The parent POM builds the `athena-jdbc` dependency from the submodule first, then the Databricks connector.

### Deploy Using SAM CLI

```bash
sam build -t connectors/athena-databricks-connector/athena-databricks-connector.yaml && \
sam deploy --guided -t connectors/athena-databricks-connector/athena-databricks-connector.yaml
```

### CloudFormation Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| AthenaCatalogName | Lambda function name (must match pattern: `^[a-z0-9-_]{1,64}$`) | databricks |
| SpillBucket | S3 bucket for spilling data | Required |
| SpillPrefix | Prefix within SpillBucket | athena-spill |
| LambdaTimeout | Maximum Lambda invocation runtime (1-900 seconds) | 900 |
| LambdaMemory | Lambda memory in MB (128-3008) | 1024 |
| SecretName | Name of the Secrets Manager secret containing the Databricks personal access token | Required |
| DatabricksDefaultDatabase | Default Databricks Unity Catalog database (catalog.schema) | default |
| DisableSpillEncryption | Disable encryption for spilled data | false |

### Update Lambda Function

For subsequent code updates after initial deployment, build and push the Docker image manually:

```bash
cd connectors && mvn clean package -DskipTests && \
cd athena-databricks-connector && \
finch build -t databricks-connector . && \
finch tag databricks-connector:latest <account-id>.dkr.ecr.<region>.amazonaws.com/<repo-name>:latest && \
finch push <account-id>.dkr.ecr.<region>.amazonaws.com/<repo-name>:latest && \
aws lambda update-function-code \
  --function-name databricks \
  --image-uri <account-id>.dkr.ecr.<region>.amazonaws.com/<repo-name>:latest \
  --region <region>
```

## Secrets Manager Configuration

The connector authenticates with Databricks using a personal access token (PAT) stored in AWS Secrets Manager. The token is retrieved at runtime by the Federation SDK — it is never embedded in code or environment variables.

### How it works

The connector's JDBC connection string contains a `${secret-name}` placeholder. At runtime, the SDK:

1. Extracts the secret name from the placeholder
2. Calls Secrets Manager to retrieve the secret value
3. Injects the `username` and `password` into the JDBC connection properties
4. Strips the placeholder from the URL before connecting

### Create the secret

The secret must be a JSON object with `username` and `password` fields. For Databricks PAT auth, the username is always `token`:

```bash
aws secretsmanager create-secret \
  --name my-databricks-secret \
  --secret-string '{"username": "token", "password": "<your-databricks-personal-access-token>"}' \
  --region <region>
```

### Reference the secret in the connection string

The Lambda environment variable for the connection string should include the secret name in `${...}` syntax:

```
databricks://jdbc:databricks://<workspace-host>:443/default${my-databricks-secret}
```

The `SecretName` parameter in the SAM template must match the secret name (e.g., `my-databricks-secret`). This grants the Lambda role `secretsmanager:GetSecretValue` permission on that specific secret.

### Update the secret

To rotate or update the token:

```bash
aws secretsmanager put-secret-value \
  --secret-id my-databricks-secret \
  --secret-string '{"username": "token", "password": "<new-token>"}' \
  --region <region>
```

No redeployment needed — the connector reads the secret on each invocation.

## Run Queries

Once deployed, query Databricks data through Athena:

```sql
-- List databases
SELECT * FROM `lambda:databricks`."information_schema"."schemata";

-- Query a table
SELECT * FROM `lambda:databricks`."default"."your_table" LIMIT 10;
```

You can run queries from the Athena console or the AWS CLI:

```bash
aws athena start-query-execution \
  --query-string 'SELECT * FROM `lambda:databricks`."default"."your_table" LIMIT 10' \
  --work-group primary \
  --region <region>
```

## JDBC Driver Configuration

### Arrow and Cloud Fetch (Disabled by default)

The Databricks JDBC driver supports [Cloud Fetch](https://docs.databricks.com/en/integrations/jdbc/capability.html#cloud-fetch-in-jdbc), which downloads query results as ~20MB Arrow-serialized chunks in parallel from DBFS. While this is faster than row-by-row streaming, each in-flight chunk consumes Lambda memory. With the default thread pool of 16, this can easily exceed Lambda's memory limit (1–3GB) on large result sets.

This connector disables Arrow (`EnableArrow=0`) so results stream row-by-row via Thrift instead. Memory usage is bounded by `DatabricksFetchSize` (default: 10,000 rows per JDBC round trip).

To re-enable Cloud Fetch for higher throughput (requires more Lambda memory), set these JDBC properties in `DatabricksConstants`:

```java
props.put("EnableArrow", "1");
props.put("CloudFetchThreadPoolSize", "4"); // limit parallel downloads
```

### Fetch Size

`DatabricksFetchSize` controls how many rows the JDBC driver buffers per round trip. Higher values reduce network round trips but use more memory. The default of 10,000 is safe for Lambda at 1GB with typical row sizes (~1KB). Lower it for tables with very wide rows.

## Troubleshooting

- **Check Lambda Logs**: `aws logs tail /aws/lambda/databricks --follow --format short --region <region>`
- **Verify Permissions**: Ensure the Lambda execution role has access to Secrets Manager and the spill bucket

## Additional Resources

- [Athena Federated Query Documentation](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source.html)
- [AWS Athena Query Federation SDK](https://github.com/awslabs/aws-athena-query-federation)
- [Databricks JDBC Driver](https://docs.databricks.com/aws/en/integrations/jdbc-oss/)
