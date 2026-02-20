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


def check_env_vars(var_names, required=False):
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
    
    if required and missing:
        raise ValueError(f"Required environment variables missing: {', '.join(missing)}")
    
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


def _get_bedrock_embedding(client, text, dimensions=256, model_id="amazon.titan-embed-text-v2:0"):
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
        modelId=model_id,
        body=json.dumps({"dimensions": dimensions, "inputText": text})
    )

    # Extract embedding from response.
    response_body = json.loads(response["body"].read())
    embeddings.append(response_body["embedding"])
    return embeddings

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



def push_to_s3(path, s3_bucket, key):

    s3 = boto3.client("s3")

    s3.upload_file(
        Filename=path,
        Bucket=s3_bucket,
        Key=key
    )



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

        items.append({
            "key": row.get(key_field),
            "embedding": embedding,
            "metadata": row
        })

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
        vector = {
            "key": item["key"],
            "data": {"float32": item["embedding"]}
        }
        if "metadata" in item:
            vector["metadata"] = item["metadata"]
        vectors.append(vector)

    for i in range(0, len(vectors), batch_size):
        batch = vectors[i:i + batch_size]
        s3vectors.put_vectors(
            vectorBucketName=bucket_name,
            indexName=index_name,
            vectors=batch
        )
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


def generate_projection_stmt(col_id, base_table, columns=None, col_label=None, col_embedding=None, joins=None):
    selects = [f'{col_id} AS "~id"']
    
    if col_label:
        selects.append(f'{col_label} AS "~label"')
    
    if columns:
        for col in columns:
            col_name = col.split('.')[-1].strip('"')
            selects.append(f'{col} AS "{col_name}"')
    
    if col_embedding:
        selects.append(f'array_join(transform({col_embedding}, x -> cast(x AS varchar)), \';\') AS "embedding:vector"')
    
    select_clause = ",\n        ".join(selects)
    
    from_clause = f"{base_table}"
    if joins:
        for table, condition in joins:
            from_clause += f" join\n    {table} \n    on {condition}"
    
    return f"""SELECT
        {select_clause}
    FROM {from_clause};"""


