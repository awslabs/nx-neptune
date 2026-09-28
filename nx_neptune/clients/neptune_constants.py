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
SERVICE_ATHENA = "athena"
SERVICE_S3 = "s3"

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
RESPONSE_COMPONENT = "component"
RESPONSE_ID = "n.id"
RESPONSE_SUCCESS = "success"

# Misc
MAX_INT = 9223372036854775807

# ---------------------------------------------------------------------------
# Algorithm-parameter validation allowlists
#
# neptune.algo.* procedures take their configuration as an inline openCypher map
# literal ({key:value,...}) in the CALL clause, so those keys/values cannot be
# bound as query parameters ($n) the way MATCH/WHERE values can. To prevent
# openCypher injection through algorithm parameters they are validated/encoded
# in opencypher_builder._to_parameter_list against the sets below.
#
# Value ranges are taken from the Neptune Analytics algorithm documentation, e.g.
# https://docs.aws.amazon.com/neptune-analytics/latest/userguide/label-propagation.html
# ---------------------------------------------------------------------------

# Every algorithm-parameter key the library is allowed to emit. Any key outside
# this set is rejected (fail closed) rather than interpolated.
ALLOWED_ALGO_PARAM_KEYS = {
    PARAM_MAX_DEPTH,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_DISTANCE,
    PARAM_DAMPING_FACTOR,
    PARAM_NUM_OF_ITERATIONS,
    PARAM_NUM_SOURCES,
    PARAM_NORMALIZE,
    PARAM_TOLERANCE,
    PARAM_SEED,
    PARAM_RESOLUTION,
    PARAM_VERTEX_LABEL,
    PARAM_VERTEX_WEIGHT_PROPERTY,
    PARAM_VERTEX_WEIGHT_TYPE,
    PARAM_EDGE_LABELS,
    PARAM_LEVEL_TOLERANCE,
    PARAM_CONCURRENCY,
    PARAM_EDGE_WEIGHT_PROPERTY,
    PARAM_EDGE_WEIGHT_TYPE,
    PARAM_MAX_ITERATIONS,
    PARAM_SOURCE_NODES,
    PARAM_SOURCE_WEIGHTS,
    PARAM_WRITE_PROPERTY,
    PARAM_MAX_LEVEL,
    PARAM_ITERATION_TOLERANCE,
}

# String parameters that name a label or property (open-domain identifiers).
# openCypher permits almost any name when backtick-quoted, so these are
# backtick-escaped + wrapped rather than matched against a narrow charset.
ALGO_PARAM_IDENTIFIER_KEYS = {
    PARAM_VERTEX_LABEL,
    PARAM_WRITE_PROPERTY,
    PARAM_EDGE_WEIGHT_PROPERTY,
    PARAM_VERTEX_WEIGHT_PROPERTY,
}

# String parameters with a closed set of legal values (rejected if not a member).
PARAM_TRAVERSAL_DIRECTIONS = {
    PARAM_TRAVERSAL_DIRECTION_INBOUND,
    PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
    PARAM_TRAVERSAL_DIRECTION_BOTH,
}
PARAM_WEIGHT_TYPES = {"int", "long", "float", "double"}

ALGO_PARAM_ENUM_VALUES = {
    PARAM_TRAVERSAL_DIRECTION: PARAM_TRAVERSAL_DIRECTIONS,
    PARAM_EDGE_WEIGHT_TYPE: PARAM_WEIGHT_TYPES,
    PARAM_VERTEX_WEIGHT_TYPE: PARAM_WEIGHT_TYPES,
}

# List-valued parameters whose string elements are label/identifier-like and are
# backtick-escaped per element; numeric elements pass through.
ALGO_PARAM_LIST_KEYS = {
    PARAM_EDGE_LABELS,
    PARAM_SOURCE_NODES,
    PARAM_SOURCE_WEIGHTS,
}
