"""
Neptune constants
"""

# AWS boto3 client services
SERVICE_NA = "neptune-graph"
SERVICE_IAM = "iam"
SERVICE_STS = "sts"

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
PARAM_PERSONALIZATION = "personalization"
PARAM_NSTART = "nstart"
PARAM_DANGLING = "dangling"
PARAM_VERTEX_LABEL = "vertexLabel"
PARAM_VERTEX_WEIGHT_PROPERTY = "vertexWeightProperty"
PARAM_VERTEX_WEIGHT_TYPE = "vertexWeightType"
PARAM_EDGE_LABELS = "edgeLabels"
PARAM_CONCURRENCY = "concurrency"
PARAM_EDGE_WEIGHT_PROPERTY = "edgeWeightProperty"
PARAM_EDGE_WEIGHT_TYPE = "edgeWeightType"
PARAM_MAX_ITERATIONS = "maxIterations"
PARAM_SOURCE_NODES = "sourceNodes"
PARAM_SOURCE_WEIGHTS = "sourceWeights"
PARAM_SORT_NEIGHBORS = "sort_neighbors"
PARAM_WRITE_PROPERTY = "writeProperty"

# Internal constants for json results
RESPONSE_RANK = "rank"
RESPONSE_DEGREE = "degree"
RESPONSE_ID = "n.id"
RESPONSE_SUCCESS = "success"

# Misc
MAX_INT = 9223372036854775807
