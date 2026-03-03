/*-
 * #%L
 * athena-s3vector-connector
 * %%
 * Copyright (C) 2026 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.s3vector;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.GetVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.GetVectorsResponse;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * User-defined function handler for retrieving vector embeddings from S3 Vector storage.
 * <p>
 * This handler enables Athena queries to fetch vector embeddings by processing batch requests.
 * It supports cross-bucket and cross-index lookups by partitioning requests based on bucket and
 * index combinations.
 * <p>
 * The handler exposes a single UDF: get_embedding(bucketName, indexName, vector_id) which returns
 * the embedding as a List of Float values.
 * <p>
 * Note: Since UDF invocations is programmed to run on sequentially on SDK, this handler uses
 * processRows to pre-fetch all embeddings in batches and stores them in a lookup map that the UDF
 * method accesses during execution.
 * <p>
 * <b>Important Limitation:</b> Athena runtime batches UDF requests until the return data approaches
 * the 6MB limit per Lambda invocation. Queries returning large embeddings or high row counts may hit
 * this limit and fail. Consider limiting result set size or vector dimensions to avoid exceeding this threshold.
 * See: <a href="https://github.com/awslabs/aws-athena-query-federation/issues/1884">GitHub Issue #1884</a>
 */
public class S3VectorUserDefinedFuncHandler
        extends UserDefinedFunctionHandler
{
    private static final Logger logger = LoggerFactory.getLogger(S3VectorUserDefinedFuncHandler.class);

    private static final String SOURCE_TYPE = "S3 Vector";

    private static final int BATCH_SIZE = 60;

    private static final String ARG_ORDER_BUCKET_NAME = "0";

    private static final String ARG_ORDER_INDEX_NAME = "1";

    private static final String ARG_ORDER_VECTOR_ID = "2";

    protected final S3VectorsClient vectorsClient;

    private Map<String, List<Float>> batchLookupMap;

    public S3VectorUserDefinedFuncHandler()
    {
        super(SOURCE_TYPE);
        this.vectorsClient = S3VectorsClient.create();
    }


    /**
     * Retrieves the embedding vector for a given vector ID from the pre-fetched lookup map.
     * This method is called for each row during UDF execution after processRows has populated the map.
     * 
     * @param bucketName The S3 vector bucket name
     * @param indexName The index name within the bucket
     * @param vector_id The unique identifier for the vector
     * @return List of Float values representing the embedding, or empty list if not found
     */
    public List<Float> get_embedding(String bucketName, String indexName, String vector_id)
    {
        String key = buildLookupKey(bucketName, indexName, vector_id);
        if (!batchLookupMap.containsKey(key)) {
            logger.debug("Embedding entry [{}] not found.", key);
            return Collections.emptyList();
        } else {
            return batchLookupMap.get(key);
        }
    }


    /**
     * Intercepts and processes all input rows before UDF invocation to pre-fetch embeddings in batches.
     * This is a workaround since there's no hook access request context during individual UDF calls. The method partitions
     * requests by bucket/index, fetches embeddings in batches, and populates a lookup map that get_embedding
     * accesses during execution.
     * 
     * @param allocator Block allocator for memory management
     * @param udfMethod The UDF method to invoke
     * @param inputRecords Input block containing bucket names, index names, and vector IDs
     * @param outputSchema Schema for the output block
     * @return Processed output block
     * @throws Exception if processing fails
     */
    @Override
    protected Block processRows(BlockAllocator allocator, Method udfMethod, Block inputRecords, Schema outputSchema) throws Exception {
        // Clear previous batch to release memory immediately
        batchLookupMap = null;

        logger.trace("Intercepting UDF request with size: {}",
                inputRecords.getRowCount());

        // Place IDs into appropriate partition
        var partitions = getVectorIdsMap(inputRecords);
        Map<String, List<Float>> results = new HashMap<>();
        // Resolve embedding as per partition / batch
        for (var entry : partitions.entrySet()) {
            var partitionResult = fetchEmbeddings(entry.getKey(), entry.getValue());
            results.putAll(partitionResult);
        }

        logger.debug("Complete vector look up with size: {}", results.size());
        batchLookupMap = results;

        return super.processRows(allocator, udfMethod, inputRecords, outputSchema);
    }

    /**
     * Extracts and partitions vector IDs from input records by bucket and index.
     * 
     * @param inputRecords Input block containing bucket names, index names, and vector IDs
     * @return Map of BatchPartition to list of vector IDs for that partition
     */
    private static Map<BatchPartition, List<String>> getVectorIdsMap(Block inputRecords) {
        VarCharVector vector_bucket = (VarCharVector) inputRecords.getFieldVector(ARG_ORDER_BUCKET_NAME);
        VarCharVector vector_index = (VarCharVector) inputRecords.getFieldVector(ARG_ORDER_INDEX_NAME);
        VarCharVector vector_id = (VarCharVector) inputRecords.getFieldVector(ARG_ORDER_VECTOR_ID);

        Map<BatchPartition, List<String>> partitions = new HashMap<>();
        for (int i = 0; i < inputRecords.getRowCount(); i++) {
            String bucket = vector_bucket.getObject(i).toString();
            String index = vector_index.getObject(i).toString();
            String id = vector_id.getObject(i).toString();

            partitions.computeIfAbsent(new BatchPartition(bucket, index),
                    k -> new ArrayList<>()).add(id);
        }
        return partitions;
    }

    /**
     * Fetches embeddings from S3 Vector storage for an entire partition in batches.
     * 
     * @param partition The batch partition containing bucket and index information
     * @param vectorIds List of all vector IDs for this partition
     * @return Map of composite keys (bucket:index:id) to embedding vectors
     */
    private Map<String, List<Float>> fetchEmbeddings(BatchPartition partition, List<String> vectorIds) {
        Map<String, List<Float>> results = new HashMap<>((int) (vectorIds.size() / 0.75) + 1);
        
        for (int i = 0; i < vectorIds.size(); i += BATCH_SIZE) {
            int endIndex = Math.min(i + BATCH_SIZE, vectorIds.size());
            List<String> batchIds = vectorIds.subList(i, endIndex);
            
            try {
                var request = GetVectorsRequest.builder()
                        .vectorBucketName(partition.getBucketName())
                        .indexName(partition.getIndexName())
                        .keys(batchIds)
                        .returnData(true)
                        .returnMetadata(false)
                        .build();

                GetVectorsResponse response = vectorsClient.getVectors(request);
                var batchResult = response.vectors().stream()
                        .collect(Collectors.toMap(
                                item -> buildLookupKey(
                                        partition.getBucketName(), partition.getIndexName(), item.key()),
                                item -> item.data().float32()
                        ));
                
                logger.debug("Fetched {} vectors from {}:{}", batchResult.size(), 
                        partition.getBucketName(), partition.getIndexName());
                results.putAll(batchResult);
            } catch (Exception e) {
                logger.error("Failed to fetch vectors from {}:{} for batch [{}-{}]: {}",
                        partition.getBucketName(), partition.getIndexName(), i, endIndex, e.getMessage(), e);
            }
        }
        
        return results;
    }

    /**
     * Builds a composite lookup key from bucket, index, and vector ID.
     * 
     * @param bucketName The S3 vector bucket name
     * @param indexName The index name
     * @param vectorId The vector ID
     * @return Composite key in format "bucket:index:id"
     */
    private static String buildLookupKey(String bucketName, String indexName, String vectorId) {
        return bucketName + ":" + indexName + ":" + vectorId;
    }


}
