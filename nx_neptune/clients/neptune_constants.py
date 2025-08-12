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
"""
Neptune constants
"""

# AWS boto3 client services
SERVICE_NA = "neptune-graph"
SERVICE_IAM = "iam"
SERVICE_STS = "sts"

# APP_ID
APP_ID_NX = "nx-neptune"

# Internal constants for parameters
PARAM_MAX_DEPTH = "maxDepth"
PARAM_TRAVERSAL_DIRECTION = "traversalDirection"
PARAM_TRAVERSAL_DIRECTION_BOTH = "both"
PARAM_TRAVERSAL_DIRECTION_INBOUND = "inbound"
PARAM_TRAVERSAL_DIRECTION_OUTBOUND = "outbound"

PARAM_DISTANCE = "distance"
PARAM_DAMPING_FACTOR = "dampingFactor"
PARAM_NUM_OF_ITERATIONS = "numOfIterations"
PARAM_NUM_SOURCES = "numSources"
PARAM_NORMALIZE = "normalize"
PARAM_TOLERANCE = "tolerance"
PARAM_WEIGHT = "weight"
PARAM_SEED = "seed"
PARAM_RESOLUTION = "resolution"
PARAM_PERSONALIZATION = "personalization"
PARAM_NSTART = "nstart"
PARAM_DANGLING = "dangling"
PARAM_VERTEX_LABEL = "vertexLabel"
PARAM_VERTEX_WEIGHT_PROPERTY = "vertexWeightProperty"
PARAM_VERTEX_WEIGHT_TYPE = "vertexWeightType"
PARAM_EDGE_LABELS = "edgeLabels"
PARAM_LEVEL_TOLERANCE = "levelTolerance"
PARAM_CONCURRENCY = "concurrency"
PARAM_EDGE_WEIGHT_PROPERTY = "edgeWeightProperty"
PARAM_EDGE_WEIGHT_TYPE = "edgeWeightType"
PARAM_MAX_ITERATIONS = "maxIterations"
PARAM_SOURCE_NODES = "sourceNodes"
PARAM_SOURCE_WEIGHTS = "sourceWeights"
PARAM_SORT_NEIGHBORS = "sort_neighbors"
PARAM_WRITE_PROPERTY = "writeProperty"
PARAM_MAX_LEVEL = "maxLevels"
PARAM_ITERATION_TOLERANCE = "iterationTolerance"

# Internal constants for json results
RESPONSE_RANK = "rank"
RESPONSE_DEGREE = "degree"
RESPONSE_ID = "n.id"
RESPONSE_SUCCESS = "success"

# Misc
MAX_INT = 9223372036854775807
