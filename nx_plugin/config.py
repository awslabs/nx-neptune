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
import os
from dataclasses import dataclass
from typing import Optional

from networkx.utils.configs import Config

__all__ = [
    "NeptuneConfig",
    "_config",
    "NETWORKX_GRAPH_ID",
    "NETWORKX_S3_IAM_ROLE_ARN",
]

NETWORKX_GRAPH_ID = os.environ.get("NETWORKX_GRAPH_ID")
NETWORKX_S3_IAM_ROLE_ARN = os.environ.get("NETWORKX_S3_IAM_ROLE_ARN")


@dataclass
class NeptuneConfig(Config):
    """
    Configuration for NX-NEPTUNE backend that controls how the backend behaves when connecting to Neptune Analytics.

    Attribute and bracket notation are supported for getting and setting configurations::

        >>> neptune_config = nx.config.backends.neptune

    General Parameters
    ------------------
    Neptune Analytics instance configuration fields:

    graph_id: Optional[str]
        use graph by graph_id; overridden by the environment variable NETWORKX_GRAPH_ID.  Otherwise, defaults to None.

    s3_iam_role: Optional[str]
        IAM role for Neptune Analytics to use to import/export from S3. Overridden by NETWORKX_S3_IAM_ROLE_ARN.
        Otherwise, defaults to None.

    Neptune Analytics Setup Parameters
    ----------------------------------
    These settings are used for instance creation and data restoration.

    create_instance: bool | dict
        If True and if graph_id is not provided, create a new instance before call. Defaults to False.
        If set to a dictionary, it will be used as the custom configuration for instance creation.
        For configuration details, refer to:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/neptune-graph/client/create_graph.html

        Example:

        nx.config.backends.neptune.create_new_instance = {
            "provisionedMemory": 32,
            "tags": {"additional_tag": "custom_tag"},
        }


    import_s3_bucket: Optional[str]
        S3 bucket with files to import before call. Defaults to None.

    restore_snapshot: Optional[str]
        Neptune Analytics snapshot instance to restore before call. Defaults to None.

    skip_graph_reset: bool
        If graph_id is defined and set to True, importing from S3 requires that the graph is empty. This resets the
        graph prior to import. Defaults to False.

    Neptune Analytics Teardown Parameters:
    ----------------------------------
    These settings are used after the algorithm call is complete, to persist data and destroy the instance.

    export_s3_bucket: Optional[str]
        If defined, saves the graph to this S3 bucket as a Gremlin CSV. Defaults to None.

    save_snapshot: bool
        If True, saves the Neptune Analytics instance as a snapshot. Defaults to False.

    destroy_instance: bool
        If True, destroys the instance. Defaults to False.

    reset_graph: bool
        If True, resets the graph. Defaults to False.

    Notes
    -----
    Environment variables may be used to control some default configurations:

        NETWORKX_GRAPH_ID: If graph_id is not provided in the configuration, use this value instead.
        NETWORKX_S3_IAM_ROLE_ARN: If s3_iam_role is not provided in the configuration, use this value instead.

    This is a global configuration. Use with caution when using from multiple threads.
    """

    active: bool = False
    backend: str = "neptune"
    verbose: int = 0
    prefer: bool = False
    require: bool = False

    ############################################
    # General settings for Neptune Analytics:
    graph_id: Optional[str] = None
    s3_iam_role: Optional[str] = None
    # role_arn
    skip_graph_reset: bool = False

    ############################################
    # Settings for Neptune Analytics setup:

    # Feature flag to control whether the module should create a Neptune Analytics instance on the user's behalf.
    # If custom configuration is needed, the user can provide a dictionary instead of a boolean.
    # For detailed configuration options, see:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/neptune-graph/client/create_graph.html
    create_new_instance: bool | dict = False
    # create_instance
    restore_snapshot: Optional[str] = None
    import_s3_bucket: Optional[str] = None
    # s3_import_path

    ############################################
    # Settings for Neptune Analytics teardown:
    export_s3_bucket: Optional[str] = None
    save_snapshot: bool = False
    destroy_instance: bool = False
    reset_graph: bool = False

    ############################################
    # Configuration for batch inserts:
    # When the number of incoming graph nodes or edges exceeds the specified thresholds,
    # the update process will be performed in chunks to optimize performance and resource usage.
    batch_update_node_size: int = 20000
    batch_update_edge_size: int = 10000

    def validate_config(self):
        """Validate the Neptune configuration."""
        # Validate setup
        if self.graph_id is None and self.create_new_instance is False:
            raise ValueError(
                "Configuration error: create_new_instance is False and graph_id is None.  Either set "
                "create_new_instance to True or provide a graph_id in the configuration to connect to an existing "
                "Neptune Analytics instance."
            )

        if self.graph_id is not None and self.create_new_instance is True:
            raise ValueError(
                "Configuration error: graph_id is defined and create_new_instance is True.  Either set graph_id to "
                "None or set create_new_instance to False."
            )

        if self.import_s3_bucket is not None and self.s3_iam_role is None:
            raise ValueError(
                "Configuration error: import_s3_bucket is defined without an ARN role to execute the task.  When "
                "import_s3_bucket is defined, s3_iam_role must be defined in the configuration."
            )

        if self.import_s3_bucket is not None and self.restore_snapshot is not None:
            raise ValueError(
                "Configuration error: both import_s3_bucket is defined and restore_snapshot is defined.  Only one of "
                "these options may be defined in the configuration."
            )

        if (
            self.graph_id is not None
            and self.import_s3_bucket is not None
            and self.skip_graph_reset is False
        ):
            raise ValueError(
                "Configuration error: import_s3_bucket may override existing data.  Set skip_graph_reset to True "
                "to force import."
            )

        # Validate teardown
        if self.export_s3_bucket is not None and self.s3_iam_role is None:
            raise ValueError(
                "Configuration error: export_s3_bucket is defined without an ARN role to execute the task.  When "
                "export_s3_bucket is defined, s3_iam_role must be defined in the configuration."
            )

        if self.destroy_instance is True and self.reset_graph is True:
            raise ValueError(
                "Configuration error: destroy_instance and reset_graph are both True.  Only one of these options may "
                "be True in the configuration."
            )

    def validate_mutate_execution_config(self):
        """Validate the Neptune configuration to make sure mutated graph's state stay after the execution."""
        if self.destroy_instance is True and self.export_s3_bucket is None:
            raise ValueError(
                "Configuration error: The mutate variant of the algorithm does not support persisting changes."
                "To maintain the mutated graph's state, "
                "either disable the destroy_instance option or enable the export_s3_bucket option."
            )


_config = NeptuneConfig()
