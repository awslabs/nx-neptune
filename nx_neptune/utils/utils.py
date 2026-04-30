# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import csv
import json
import logging
import os
import re
import sys
from itertools import islice
from time import sleep
from typing import List, Optional

import boto3


def get_stdout_logger(
    project_identifier: str,
    debug_modules: Optional[List[str]] = None,
    default_level: int = logging.WARNING,
    with_logger_name=False,
):
    """
    Creates and configures a logger that outputs to stdout.

    This function sets up a logger with the specified project identifier and
    configures it to output log messages to standard output (stdout). The default
    log level can be specified (defaults to WARNING), and specific modules can be
    set to DEBUG level.

    Parameters
    ----------
    project_identifier : str
        The name to identify this logger, typically the project or module name.
    debug_modules : Optional[List[str]], default=None
        A list of module names for which to set the logging level to DEBUG.
        If None, no modules will have their log level changed.
    default_level : int, default=logging.WARNING
        The default logging level to use for the logger.
        Common values: logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR
    with_logger_name : bool, default=False
        If True, includes the logger name in the log format.

    Returns
    -------
    logging.Logger
        A configured logger instance that outputs to stdout.

    Examples
    --------
    >>> logger = get_stdout_logger("nx_neptune")
    >>> logger.warning("This is a warning message")
    WARNING - This is a warning message

    >>> info_logger = get_stdout_logger("nx_neptune", default_level=logging.INFO)
    >>> info_logger.info("This info message will be displayed")
    INFO - This info message will be displayed

    >>> debug_logger = get_stdout_logger("nx_neptune", debug_modules=["nx_neptune.client"])
    >>> # The nx_neptune.client module will now log at DEBUG level
    """

    default_format = "%(levelname)s - %(message)s"
    logger_format = (
        "%(name)s - " + default_format if with_logger_name else default_format
    )

    logging.basicConfig(
        level=default_level,
        format=logger_format,
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,  # Explicitly set output to stdout
    )
    if debug_modules:
        for logger_name in debug_modules:
            logging.getLogger(logger_name).setLevel(logging.DEBUG)
    return logging.getLogger(project_identifier)


def validate_and_get_env(var_names):
    """Check environment variables and return their values.

    This function checks for the existence of specified environment variables,
    prints warnings for missing variables, and raises an error if any variables
    are missing.

    Parameters
    ----------
    var_names : list
        List of environment variable names to check.

    Returns
    -------
    dict
        Dictionary mapping variable names to their values (or None if missing).

    Raises
    ------
    ValueError
        If any environment variables are missing.

    Examples
    --------
    >>> # Check environment variables
    >>> values = validate_and_get_env(['HOME', 'USER'])
    Using HOME: /home/user
    Using USER: user
    >>> print(values)
    {'HOME': '/home/user', 'USER': 'user'}

    >>> # Missing environment variables will raise an error
    >>> values = validate_and_get_env(['AWS_REGION'])
    Warning: Environment Variable AWS_REGION is not defined
    You can set it using: %env AWS_REGION=your-value
    ValueError: Required environment variables missing: AWS_REGION
    """
    values = {}
    missing = []
    for var_name in var_names:
        value = os.getenv(var_name)
        if not value:
            print(f"Warning: Environment Variable {var_name} is not defined")
            print(f"You can set it using: %env {var_name}=your-value")
            missing.append(var_name)
        else:
            print(f"Using {var_name}: {value}")
        values[var_name] = value

    if missing:
        raise ValueError(
            f"Required environment variables missing: {', '.join(missing)}"
        )

    return values


def read_csv(path, limit=None):
    """Reads CSV file and returns header and rows.

    Parameters
    ----------
    path : str
        Path to the CSV file to read
    limit : int, optional
        Maximum number of rows to read. If None, reads entire file.

    Returns
    -------
    tuple
        A tuple containing:
            - header (list): List of column names from the CSV
            - rows (list): List of dictionaries, where each dictionary represents a row
              with column names as keys and cell values as values

    Examples
    --------
    >>> header, rows = read_csv("data.csv")
    >>> print(header)
    ['col1', 'col2', 'col3']
    >>> print(rows[0])
    {'col1': 'val1', 'col2': 'val2', 'col3': 'val3'}

    >>> # Read only first 10 rows
    >>> header, rows = read_csv("data.csv", limit=10)
    """
    with open(path, newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        rows = list(islice(reader, limit)) if limit else list(reader)
        header = list(reader.fieldnames)
    return header, rows


def write_csv(path, headers, rows):
    """Writes data to a CSV file.

    Parameters
    ----------
    path : str
        Path to the CSV file to write
    headers : list
        List of column names to use as CSV headers
    rows : list
        List of dictionaries, where each dictionary represents a row with
        column names as keys and cell values as values

    Examples
    --------
    >>> headers = ['col1', 'col2']
    >>> rows = [{'col1': 'val1', 'col2': 'val2'},
    ...         {'col1': 'val3', 'col2': 'val4'}]
    >>> write_csv('output.csv', headers, rows)
    """
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _get_bedrock_embedding(
    client, text, dimensions=256, model_id="amazon.titan-embed-text-v2:0"
):
    """
    Generate vector embeddings using Amazon Bedrock.

    Args:
        client: Bedrock runtime client instance
        text (str): Input text to generate embeddings for
        dimensions (int, optional): Size of embedding vector. Defaults to 256.
        model_id (str, optional): Bedrock model ID. Defaults to "amazon.titan-embed-text-v2:0".

    Returns:
        list: List containing the embedding vector
    """
    # Generate vector embeddings.
    embeddings = []

    response = client.invoke_model(
        modelId=model_id, body=json.dumps({"dimensions": dimensions, "inputText": text})
    )

    # Extract embedding from response.
    response_body = json.loads(response["body"].read())
    embeddings.append(response_body["embedding"])
    return embeddings


def push_to_s3(path, s3_bucket, key):
    """Uploads a file to Amazon S3.

    This function uploads a local file to an S3 bucket using the specified key.

    Parameters
    ----------
    path : str
        Local file path to upload
    s3_bucket : str
        Name of the S3 bucket to upload to
    key : str
        S3 object key (path) where the file will be stored

    Examples
    --------
    >>> push_to_s3("local_file.csv", "my-bucket", "data/file.csv")
    # Uploads local_file.csv to s3://my-bucket/data/file.csv
    """

    s3 = boto3.client("s3")
    s3.upload_file(Filename=path, Bucket=s3_bucket, Key=key)


def to_embedding_entries(rows, text_fields, key_field="id"):
    """
    Generate embeddings and return items compatible with batch_insert_vectors.

    Args:
        rows: List of row dicts
        text_fields: List of field names to concatenate for embedding
        key_field: Field to use as vector key (default 'id')

    Returns:
        List of items with 'key', 'embedding', and 'metadata' for batch_insert_vectors
    """
    bedrock = boto3.client("bedrock-runtime")
    items = []

    for row in rows:
        text = "".join(str(row.get(field, "")) for field in text_fields)
        embedding = _get_bedrock_embedding(bedrock, text)[0]

        items.append(
            {"key": row.get(key_field), "embedding": embedding, "metadata": row}
        )

    return items


def push_to_s3_vector(items, bucket_name, index_name, batch_size=300):
    """
    Insert vectors into S3 Vectors in batches.

    Args:
        items: List of dicts with 'key' and 'embedding' fields, optional 'metadata'
        bucket_name: S3 Vectors bucket name
        index_name: Index name
        batch_size: Number of vectors per batch (default 300)
    """
    s3vectors = boto3.client("s3vectors")
    vectors = []
    for item in items:
        vector = {"key": item["key"], "data": {"float32": item["embedding"]}}
        if "metadata" in item:
            vector["metadata"] = item["metadata"]
        vectors.append(vector)

    for i in range(0, len(vectors), batch_size):
        batch = vectors[i : i + batch_size]
        s3vectors.put_vectors(
            vectorBucketName=bucket_name, indexName=index_name, vectors=batch
        )
        print(f"Inserted batch {i // batch_size + 1}: {len(batch)} vectors")


def push_to_opensearch(items, index_name, client=None, batch_size=500, recreate=False):
    """
    Insert vectors into OpenSearch in batches using bulk API.

    Note: This function is for demo purposes only and not intended for production use.

    Args:
        items: List of dicts with 'key' and 'embedding' fields, optional 'metadata'
        client: OpenSearch client instance or None to create from env vars
        index_name: Index name
        batch_size: Number of vectors per batch (default 500)
        recreate: If True, recreate index before inserting (default False)
    """
    from opensearchpy import OpenSearch, helpers

    # Create client if not provided
    if client is None:
        host = os.getenv("OPEN_SEARCH_ENDPOINT")
        user_name = os.getenv("OPEN_SEARCH_USER")
        password = os.getenv("OPEN_SEARCH_PASSWORD")
        client = OpenSearch(hosts=[host], http_auth=(user_name, password), use_ssl=True)

    # Create or recreate index
    mapping = {
        "mappings": {
            "_meta": {"embedding": "list"},
            "properties": {"id": {"type": "keyword"}, "embedding": {"type": "float"}},
        }
    }

    if client.indices.exists(index=index_name):
        if recreate:
            client.indices.delete(index=index_name)
            client.indices.create(index=index_name, body=mapping)
    else:
        client.indices.create(index=index_name, body=mapping)

    actions = []
    for item in items:
        action = {
            "_index": index_name,
            "_id": item["key"],
            "_source": {"id": item["key"], "embedding": item["embedding"]},
        }
        actions.append(action)

    for i in range(0, len(actions), batch_size):
        batch = actions[i : i + batch_size]
        helpers.bulk(client, batch)
        print(f"Inserted batch {i // batch_size + 1}: {len(batch)} vectors")


def generate_create_table_ddl(table_name, s3_location, columns):
    """
    Generate CREATE EXTERNAL TABLE DDL statement for Athena.

    Args:
        table_name: Name of the table to create
        s3_location: S3 location for the table data
        columns: List of tuples (column_name, column_type)

    Returns:
        str: DDL statement

    Example:
        >>> columns = [("id", "string"), ("name", "string"), ("embedding", "array<float>")]
        >>> ddl = generate_create_table_ddl("my_table", "s3://bucket/path/", columns)
    """
    _validate_sql_identifier(table_name)
    column_defs = ",\n    ".join([f"`{name}` {dtype}" for name, dtype in columns])

    return f"""CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
    {column_defs}
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES ('field.delim' = ',', 'collection.delim' = ';')
    STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '{s3_location}'
    TBLPROPERTIES ('classification' = 'csv', 'skip.header.line.count'='1');
    """


def generate_projection_stmt(
    col_id,
    base_table,
    columns=None,
    col_label=None,
    col_embedding=None,
    joins=None,
    connector_name=None,
    vector_bucket=None,
    vector_index=None,
    col_vector_id=None,
):
    """
    Generate a SQL SELECT statement for projecting data with Neptune-compatible column names.

    This function creates a SQL projection statement that formats columns for Neptune graph
    database ingestion, including special handling for ID, label, and embedding columns.

    Parameters
    ----------
    col_id : str
        Column name to use as the vertex/edge ID, will be aliased as "~id"
    base_table : str
        Name of the base table to select from
    columns : list of str, optional
        List of additional column names to include in the projection.
        Column names will be extracted from qualified names (e.g., "table.column" -> "column")
    col_label : str, optional
        Column name to use as the vertex/edge label, will be aliased as "~label"
    col_embedding : str, optional
        Column name containing embedding array data, will be formatted as semicolon-separated
        string and aliased as "embedding:vector"
    joins : list of tuple, optional
        List of (table_name, join_condition) tuples for joining additional tables

    Returns
    -------
    str
        A formatted SQL SELECT statement with appropriate column aliases and joins

    Examples
    --------
    >>> stmt = generate_projection_stmt("id", "users", columns=["name", "age"])
    >>> print(stmt)
    SELECT
        id AS "~id",
        name AS "name",
        age AS "age"
    FROM users;

    >>> stmt = generate_projection_stmt(
    ...     "u.id", "users u",
    ...     columns=["u.name", "p.title"],
    ...     col_label="u.type",
    ...     joins=[("posts p", "u.id = p.user_id")]
    ... )
    >>> print(stmt)
    SELECT
        u.id AS "~id",
        u.type AS "~label",
        u.name AS "name",
        p.title AS "title"
    FROM users u join
        posts p
        on u.id = p.user_id;
    """
    for part in base_table.split():
        _validate_sql_identifier(part)
    selects = [f'{col_id} AS "~id"']

    if col_label:
        selects.append(f'{col_label} AS "~label"')

    if columns:
        for col in columns:
            col_name = col.split(".")[-1].strip('"')
            selects.append(f'{col} AS "{col_name}"')

    if col_embedding:
        selects.append(f"array_join({col_embedding}, ';') AS \"embedding:vector\"")

    if col_vector_id:
        selects.append(
            "array_join(transform(get_embedding("
            f"'{vector_bucket}', '{vector_index}', {col_vector_id}"
            "), x -> cast(x AS varchar)), ';') AS \"embedding:vector\""
        )

    select_clause = ",\n        ".join(selects)

    from_clause = f"{base_table}"
    if joins:
        for table, condition in joins:
            from_clause += f" join\n    {table} \n    on {condition}"

    if connector_name:
        return f"""
            USING
            EXTERNAL FUNCTION get_embedding(schema_name VARCHAR, index_name VARCHAR, id VARCHAR )
                RETURNS ARRAY<REAL>
            LAMBDA '{connector_name}'
            SELECT
                {select_clause},
                row_number() OVER () AS bucket
            FROM {from_clause};"""
    else:
        return f"""
          SELECT
              {select_clause}
          FROM {from_clause};"""


def _validate_sql_identifier(value: str) -> str:
    """Validate that *value* is a safe SQL identifier (table or column name).

    Accepts dotted names like ``catalog.database.table``.

    Raises ``ValueError`` if the value contains characters that could
    enable SQL injection.
    """
    _SQL_IDENTIFIER_RE = re.compile(
        r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*\Z"
    )
    if not value or not _SQL_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Invalid SQL identifier: {value!r}. "
            "Only letters, digits, underscores, and dots are allowed."
        )
    return value
