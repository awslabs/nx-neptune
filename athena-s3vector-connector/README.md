## Athena S3 Vector Connector

This connector enables Amazon Athena to query vector data stored in Amazon S3 using the S3 Vector API. The connector allows you to perform federated queries on vector embeddings and related data directly from Athena.

## Connector Status: Preview

The S3 Vector Athena connector is currently in **preview** and available only as source code for building locally. This is not a production-ready release.

We're releasing this connector as an open-source preview to:
- Gather community feedback on functionality and use cases
- Collaborate on the project roadmap
- Identify issues and improvements before general availability

**What this means:**
- Build and deploy from source using the instructions below
- Recommended for testing and evaluation purposes only
- Not yet released to public artifact repositories (Maven Central, etc.)

We welcome questions, suggestions, and contributions from the community.

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

### Update Lambda Function

For subsequent updates after initial deployment:

1. **Upload JAR to S3:**
   ```bash
   aws s3 cp target/athena-s3vector-connector-0.1.0.jar s3://<your-bucket>/
   ```

2. **Update Lambda function code:**
   ```bash
   aws lambda update-function-code \
     --function-name <your-function-name> \
     --s3-bucket <your-bucket> \
     --s3-key athena-s3vector-connector-0.1.0.jar
   ```

*Note: A temporary S3 bucket is required for this update process as the JAR file exceeds 50 MB, which is the direct upload limit for Lambda functions.*

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

#### Example Federated Query

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

### Using User-Defined Function (UDF)

The connector also provides a UDF for retrieving vector embeddings that can be used with any Athena table or query.

#### UDF Signature

```sql
get_embedding(bucket_name VARCHAR, index_name VARCHAR, vector_id VARCHAR) RETURNS ARRAY<REAL>
```

**Parameters:**
- `bucket_name`: The S3 vector bucket name
- `index_name`: The index name within the bucket
- `vector_id`: The unique identifier for the vector

**Returns:** Array of REAL values representing the embedding vector

#### Example UDF Query

```sql
USING 
EXTERNAL FUNCTION get_embedding(bucket_name VARCHAR, index_name VARCHAR, vector_id VARCHAR) 
    RETURNS ARRAY<REAL>
LAMBDA 's3-vector'
SELECT 
    vector_id, 
    get_embedding('test-vector-bucket', 'movies', vector_id) as embedding
FROM gen;
```

**Important Notes:**
- Replace `'s3-vector'` with your Lambda function name
- The UDF batches requests automatically for efficiency
- Athena batches UDF invocations until return data approaches the 6MB Lambda limit. Large result sets may fail. Consider limiting rows or vector dimensions.
- See [GitHub Issue #1884](https://github.com/awslabs/aws-athena-query-federation/issues/1884) for more details on UDF limitations

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
