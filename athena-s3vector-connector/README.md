## Athena S3 Vector Connector

This connector enables Amazon Athena to query vector data stored in Amazon S3 using the S3 Vector API. The connector allows you to perform federated queries on vector embeddings and related data directly from Athena.

## What is the S3 Vector Connector?

The S3 Vector Connector is a specialized Athena connector that enables querying vector data stored in S3 Vector Bucket. It implements both metadata and record handling capabilities to:

1. Provide schema information about vector databases, tables, and columns stored in S3 Vector Bucket
2. Read vector data and embeddings from S3 Vector Bucket for query processing
3. Support filtering and projection operations on vector data (To-do)

The connector uses the AWS S3 Vectors SDK to efficiently access and process vector embeddings stored in S3.

## Prerequisites

Before deploying this connector, ensure you have:

- [Proper permissions/policies to deploy/use Athena Federated Queries](https://docs.aws.amazon.com/athena/latest/ug/federated-query-iam-access.html)
- An S3 bucket for spilling large query results
- An S3 vector bucket containing your vector data
- Athena workgroup configured to use Athena Engine Version 3

## How To Deploy

### Deploy Using CloudFormation Template

The connector can be deployed directly using the provided CloudFormation template:

1. **Build the connector:**
   ```bash
   mvn clean install
   ```

2. **Deploy using SAM CLI:**
   ```bash
   sam deploy --template-file athena-s3vector-connector.yaml --guided
   ```

3. **Follow the guided prompts and provide:**
   - **AthenaCatalogName**: Name for your Lambda function (lowercase, alphanumeric, hyphens, and underscores only, 1-64 characters)
   - **SpillBucket**: S3 bucket name for query result spilling
   - **SpillPrefix**: Prefix within SpillBucket (default: athena-spill)
   - **LambdaTimeout**: Maximum Lambda runtime in seconds (default: 900)
   - **LambdaMemory**: Lambda memory in MB (default: 512)
   - **DisableSpillEncryption**: Set to 'true' to disable spill encryption (default: false)

### CloudFormation Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| AthenaCatalogName | Lambda function name (must match pattern: ^[a-z0-9-_]{1,64}$) | Required |
| SpillBucket | S3 bucket for spilling data | Required |
| SpillPrefix | Prefix within SpillBucket | athena-spill |
| LambdaTimeout | Maximum Lambda invocation runtime (1-900 seconds) | 900 |
| LambdaMemory | Lambda memory in MB (128-3008) | 1024 |
| DisableSpillEncryption | Disable encryption for spilled data | false |


## Run Queries

Once deployed and validated, you can query your vector data using Athena:

### Schema Information

The connector provides a default schema with two columns:
- **vector_id** (VARCHAR): The unique identifier for each vector
- **vector** (VARCHAR): The embedding data in string representation

### Example Data Query

```sql
-- Query vector data
SELECT vector_id, vector
FROM "lambda:<function_name>".<schema_name>.<table_name>
LIMIT 10;

-- Query specific vectors by ID
SELECT vector_id, vector
FROM "lambda:<function_name>".<schema_name>.<table_name>
WHERE vector_id = 'your-vector-id';
```

*Note: Replace `<function_name>` with the name of your Lambda function.*

## Architecture

The connector consists of:

- **S3VectorMetadataHandler**: Handles metadata operations (list schemas, tables, get table definitions, partitions, and splits)
- **S3VectorRecordHandler**: Handles data reading operations from S3 vector storage
- **S3VectorCompositeHandler**: Combines both handlers into a single Lambda function for simplified deployment

## Troubleshooting

- **Check Lambda Logs**: View CloudWatch Logs for your Lambda function to diagnose issues
- **Verify Permissions**: Ensure the Lambda execution role has permissions to access both SpillBucket and DataBucket
- **Check Athena Engine Version**: Confirm your workgroup is using Athena Engine Version 2

## Additional Resources

- [Athena Federated Query Documentation](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source.html)
- [AWS Athena Query Federation SDK](https://github.com/awslabs/aws-athena-query-federation)
- [Athena Engine Versions](https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html)
