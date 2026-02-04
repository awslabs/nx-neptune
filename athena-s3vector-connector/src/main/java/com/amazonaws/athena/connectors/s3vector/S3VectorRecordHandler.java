/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.GetOutputVector;
import software.amazon.awssdk.services.s3vectors.model.GetVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.GetVectorsResponse;
import software.amazon.awssdk.services.s3vectors.model.ListOutputVector;
import software.amazon.awssdk.services.s3vectors.model.ListVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.ListVectorsResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

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

    public static final String COL_VECTOR_ID = "vector_id";

    public static final String COL_EMBEDDING_DATA = "vector";

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
        logger.warn(recordsRequest.getConstraints().getSummary().toString());

        TableName tableName = recordsRequest.getTableName();
        String table = tableName.getTableName();
        String schema = tableName.getSchemaName();

        // Configure Arrow format.
        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
        builder.withExtractor(COL_VECTOR_ID, (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = ((Map<String, String>) context).get(COL_VECTOR_ID);
        });
        builder.withExtractor(COL_EMBEDDING_DATA, (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = ((Map<String, String>) context).get(COL_EMBEDDING_DATA);
        });


        var summary = recordsRequest.getConstraints().getSummary();
        var items = new ArrayList<Map<String, String>>();

        // When user pass in conditional clause on column vector_id then avoid full tabel scan.
        if (summary.containsKey(COL_VECTOR_ID) && summary.get(COL_VECTOR_ID) instanceof SortedRangeSet) {
            List<String> ids = getIds(summary);
            items.addAll(getVectorsById(schema, table, ids).vectors().stream()
                    .map(item -> Map.of(COL_VECTOR_ID, item.key(), COL_EMBEDDING_DATA, item.data().toString()))
                    .collect(java.util.stream.Collectors.toList()));
        } else {
            items.addAll(getVectors(schema, table).vectors().stream()
                    .map(item -> Map.of(COL_VECTOR_ID, item.key(), COL_EMBEDDING_DATA, item.data().toString()))
                    .collect(java.util.stream.Collectors.toList()));
        }

        logger.warn("No. of vector entries fetched: {}", items.size());

        GeneratedRowWriter rowWriter = builder.build();
        for(Map<String, String> item : items) {
            logger.info("readWithConstraint: processing line " + item);
            spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, item) ? 1 : 0);
        }
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


    public ListVectorsResponse getVectors(String bucketName, String indexName) {

        var request = ListVectorsRequest.builder()
                .vectorBucketName(bucketName)
                .indexName(indexName)
                .returnData(true)
                .build();

        ListVectorsResponse response = vectorsClient.listVectors(request);

        logger.debug("Response from S3 vector: {}", response);

        return response;

    }

    public GetVectorsResponse getVectorsById(String bucketName, String indexName, List<String> ids) {

        var request = GetVectorsRequest.builder()
                .vectorBucketName(bucketName)
                .indexName(indexName)
                .keys(ids)
                .returnData(true)
                .build();

        GetVectorsResponse response = vectorsClient.getVectors(request);

        logger.debug("Response from Filtered S3 vector: {}", response);

        return response;

    }



}
