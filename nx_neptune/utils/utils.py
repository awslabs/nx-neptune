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


def check_env_vars(var_names):
    values = {}
    for var_name in var_names:
        value = os.getenv(var_name)
        if not value:
            print(f"Warning: Environment Variable {var_name} is not defined")
            print(f"You can set it using: %env {var_name}=your-value")
        else:
            print(f"Using {var_name}: {value}")
        values[var_name] = value
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
