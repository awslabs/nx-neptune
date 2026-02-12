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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.GetVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.GetVectorsResponse;
import software.amazon.awssdk.services.s3vectors.model.ListVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.ListVectorsResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_EMBEDDING_DATA;
import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_METADATA;
import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_VECTOR_ID;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-example) will guide you through preparing
 * your development environment, modifying this example RecordHandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with actual rows level data from your source. Athena
 * will call readWithConstraint(...) on this class for each 'Split' you generated in ExampleMetadataHandler.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class S3VectorRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(S3VectorRecordHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "S3 Vectors";
    private static final int BATCH_SIZE = 300;

    private final S3VectorsClient vectorsClient;

    public S3VectorRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(S3Client.create(), S3VectorsClient.create(), SecretsManagerClient.create(), AthenaClient.create(), configOptions);
    }

    @VisibleForTesting
    protected S3VectorRecordHandler(S3Client amazonS3, S3VectorsClient vectorsClient, SecretsManagerClient secretsManager, AthenaClient amazonAthena, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE, configOptions);

        this.vectorsClient = vectorsClient;
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     * The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest Details of the read request, including:
     * 1. The Split
     * 2. The Catalog, Database, and Table the read request is for.
     * 3. The filtering predicate (if any)
     * 4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws IOException
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because this will limit the BlockSpiller's
     * ability to control Block size. The resulting increase in Block size may cause failures and reduced performance.
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws IOException
    {
        TableName tableName = recordsRequest.getTableName();
        String table = tableName.getTableName();
        String schemaName = tableName.getSchemaName();
        Schema tableSchema = recordsRequest.getSchema();
        Split split = recordsRequest.getSplit();
        Map<String, ValueSet> summary = recordsRequest.getConstraints().getSummary();

        Set<String> columnNamesSst = tableSchema.getFields().stream()
                .map(Field::getName)
                .filter(c -> !split.getProperties().containsKey(c))
                .collect(Collectors.toSet());

        boolean fetchEmbedding = columnNamesSst.contains(COL_EMBEDDING_DATA);
        boolean fetchMetadata = columnNamesSst.contains(COL_METADATA);
        boolean selectByIds = summary.containsKey(COL_VECTOR_ID) && summary.get(COL_VECTOR_ID) instanceof SortedRangeSet;

        logger.info("Execute fetch request with config: [fetchEmbedding: {}, fetchMetadata: {}, selectByIds: {}]",
                fetchEmbedding, fetchMetadata, selectByIds);

        GeneratedRowWriter rowWriter = getRowWriter(recordsRequest);
        int totalFetched = 0;

        if (selectByIds) {
            List<String> allIds = getIds(summary);
            // Process IDs in batches of 300
            for (int i = 0; i < allIds.size(); i += BATCH_SIZE) {
                if (!queryStatusChecker.isQueryRunning()) {
                    logger.info("Query cancelled, stopping fetch");
                    break;
                }
                
                List<String> batchIds = allIds.subList(i, Math.min(i + BATCH_SIZE, allIds.size()));
                List<VectorData> items = getVectorsById(schemaName, table, batchIds, fetchEmbedding, fetchMetadata);
                
                for (VectorData item : items) {
                    spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, item) ? 1 : 0);
                }
                totalFetched += items.size();
            }
        } else {
            // Paginate through all vectors
            String nextToken = null;
            do {
                if (!queryStatusChecker.isQueryRunning()) {
                    logger.info("Query cancelled, stopping fetch");
                    break;
                }
                
                VectorPage page = getVectorsPage(schemaName, table, nextToken, fetchEmbedding, fetchMetadata);
                
                for (VectorData item : page.getVectors()) {
                    spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, item) ? 1 : 0);
                }
                
                totalFetched += page.getVectors().size();
                nextToken = page.getNextToken();
            } while (nextToken != null);
        }

        logger.info("Total vector entries fetched: {}", totalFetched);
    }

    private static GeneratedRowWriter getRowWriter(ReadRecordsRequest recordsRequest) {

        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
        // Field: ID
        builder.withExtractor(COL_VECTOR_ID, (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = ((VectorData) context).getId();
        });
        // Field: Embedding
        builder.withFieldWriterFactory(COL_EMBEDDING_DATA,
            (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                (Object context, int rowNum) -> {
                    ((VectorData) context).getEmbedding().ifPresent(
                            embedding -> BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, embedding));
                    return true;
                });
        // Field: Metadata
        builder.withExtractor(COL_METADATA, (VarCharExtractor) (Object context, NullableVarCharHolder value) ->
                ((VectorData) context).getMetadata().ifPresent(
            metadata -> {
                value.isSet = 1;
                value.value = metadata;
            }
        ));
        return builder.build();
    }

    private static List<String> getIds(Map<String, ValueSet> summary) {
        List<String> ids = new ArrayList<>();
        SortedRangeSet rangeSet = (SortedRangeSet) summary.get(COL_VECTOR_ID);
        rangeSet.getOrderedRanges().forEach(range -> {
            if (range.getLow().getBound() == Marker.Bound.EXACTLY) {
                ids.add(range.getLow().getValue().toString());
                logger.debug("Adding ID: {}", range.getLow().getValue().toString());
            }
        });
        return ids;
    }

    /**
     * Retrieves a single page of vectors from the specified S3 vector bucket and index.
     *
     * @param bucketName The name of the S3 vector bucket
     * @param indexName The name of the vector index
     * @param nextToken Pagination token, null for first page
     * @param fetchEmbedding Whether to fetch embedding data
     * @param fetchMetadata Whether to fetch metadata
     * @return VectorPage containing vectors and next token
     */
    private VectorPage getVectorsPage(String bucketName, String indexName, String nextToken, boolean fetchEmbedding, boolean fetchMetadata) {
        var requestBuilder = ListVectorsRequest.builder()
                .vectorBucketName(bucketName)
                .indexName(indexName)
                .returnData(fetchEmbedding)
                .returnMetadata(fetchMetadata);
        
        if (nextToken != null) {
            requestBuilder.nextToken(nextToken);
        }

        ListVectorsResponse response = vectorsClient.listVectors(requestBuilder.build());
        logger.debug("Fetched page with {} vectors, hasNextToken: {}", 
                response.vectors().size(), response.nextToken() != null);

        List<VectorData> vectors = response.vectors().stream()
                .map(item -> new VectorData(
                    item.key(),
                    fetchEmbedding ? item.data().float32() : null,
                    fetchMetadata ? item.metadata().toString() : null
                ))
                .collect(Collectors.toList());

        return new VectorPage(vectors, response.nextToken());
    }
    /**
     * Retrieves specific vectors by their IDs from the specified S3 vector bucket and index.
     *
     * @param bucketName The name of the S3 vector bucket
     * @param indexName The name of the vector index
     * @param ids List of vector IDs to retrieve (max 300)
     * @param fetchEmbedding Whether to fetch embedding data
     * @param fetchMetadata Whether to fetch metadata
     * @return List of VectorData containing the requested vector information
     */
    private List<VectorData> getVectorsById(String bucketName, String indexName, List<String> ids, boolean fetchEmbedding, boolean fetchMetadata) {
        var request = GetVectorsRequest.builder()
                .vectorBucketName(bucketName)
                .indexName(indexName)
                .keys(ids)
                .returnData(fetchEmbedding)
                .returnMetadata(fetchMetadata)
                .build();

        GetVectorsResponse response = vectorsClient.getVectors(request);
        logger.debug("Fetched {} vectors by ID", response.vectors().size());

        return response.vectors().stream()
                .map(item -> new VectorData(
                    item.key(),
                    fetchEmbedding ? item.data().float32() : null,
                    fetchMetadata ? item.metadata().toString() : null
                ))
                .collect(Collectors.toList());
    }

    /**
     * Helper class to hold a page of vectors with pagination token.
     */
    private static class VectorPage {
        private final List<VectorData> vectors;
        private final String nextToken;

        public VectorPage(List<VectorData> vectors, String nextToken) {
            this.vectors = vectors;
            this.nextToken = nextToken;
        }

        public List<VectorData> getVectors() {
            return vectors;
        }

        public String getNextToken() {
            return nextToken;
        }
    }

}
