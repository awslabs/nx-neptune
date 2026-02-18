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

import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_EMBEDDING_DATA;
import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_METADATA;
import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_VECTOR_ID;

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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.s3vector.fetcher.AbstractVectorFetcher;
import com.amazonaws.athena.connectors.s3vector.fetcher.IdScanVectorFetcher;
import com.amazonaws.athena.connectors.s3vector.fetcher.TableScanVectorFetcher;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

/**
 * This class is part of an tutorial that will walk you through how to build a
 * connector for your custom data source. The README for this module
 * (athena-example) will guide you through preparing your development
 * environment, modifying this example RecordHandler, building, deploying, and
 * then using your new source in an Athena query.
 *
 * <p>More specifically, this class is responsible for providing Athena with
 * actual rows level data from your source. Athena will call
 * readWithConstraint(...) on this class for each 'Split' you generated in
 * ExampleMetadataHandler.
 *
 * <p>For more examples, please see the other connectors in this repository
 * (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class S3VectorRecordHandler extends RecordHandler {
  private static final Logger logger =
      LoggerFactory.getLogger(S3VectorRecordHandler.class);

  /**
   * used to aid in debugging. Athena will use this name in conjunction with
   * your catalog id to correlate relevant query errors.
   */
  private static final String SOURCE_TYPE = "S3 Vectors";

  private final S3VectorsClient vectorsClient;

  /** Constructor for S3VectorRecordHandler. */
  public S3VectorRecordHandler(java.util.Map<String, String> configOptions) {
    this(
        S3Client.create(),
        S3VectorsClient.create(),
        SecretsManagerClient.create(),
        AthenaClient.create(),
        configOptions);
  }

  @VisibleForTesting
  protected S3VectorRecordHandler(
      S3Client amazonS3,
      S3VectorsClient vectorsClient,
      SecretsManagerClient secretsManager,
      AthenaClient amazonAthena,
      java.util.Map<String, String> configOptions) {
    super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE, configOptions);

    this.vectorsClient = vectorsClient;
  }

  /**
   * Used to read the row data associated with the provided Split.
   *
   * @param spiller A BlockSpiller that should be used to write the row data
   *     associated with this Split. The BlockSpiller automatically handles
   *     chunking the response, encrypting, and spilling to S3.
   * @param recordsRequest Details of the read request, including: 1. The Split
   *     2. The Catalog, Database, and Table the read request is for. 3. The
   *     filtering predicate (if any) 4. The columns required for projection.
   * @param queryStatusChecker A QueryStatusChecker that you can use to stop
   *     doing work for a query that has already terminated
   * @throws IOException if an error occurs
   * @note Avoid writing more than 10 rows per-call to
   *     BlockSpiller.writeRow(...) because this will limit the BlockSpiller's
   *     ability to control Block size. The resulting increase in Block size may
   *     cause failures and reduced performance.
   */
  @Override
  protected void readWithConstraint(
      BlockSpiller spiller,
      ReadRecordsRequest recordsRequest,
      QueryStatusChecker queryStatusChecker)
      throws IOException {
    Map<String, ValueSet> summary = recordsRequest.getConstraints().getSummary();
    boolean selectByIds =
        summary.containsKey(COL_VECTOR_ID)
            && summary.get(COL_VECTOR_ID) instanceof SortedRangeSet;

    AbstractVectorFetcher fetcher;
    if (selectByIds) {
      List<String> ids = getIds(summary);
      if (!ids.isEmpty()) {
        fetcher = new IdScanVectorFetcher(vectorsClient, recordsRequest, ids);
      } else {
        logger.warn("Unsupported range value, fallback to full table scan. ");
        fetcher = new TableScanVectorFetcher(vectorsClient, recordsRequest);
      }
    } else {
      fetcher = new TableScanVectorFetcher(vectorsClient, recordsRequest);
    }

    GeneratedRowWriter rowWriter = getRowWriter(recordsRequest);
    int totalFetched = 0;

    while (fetcher.hasNext() && queryStatusChecker.isQueryRunning()) {
      List<VectorData> batch = fetcher.next();
      for (VectorData item : batch) {
        spiller.writeRows(
            (Block block, int rowNum) ->
                rowWriter.writeRow(block, rowNum, item) ? 1 : 0);
      }
      totalFetched += batch.size();
    }

    if (!queryStatusChecker.isQueryRunning()) {
      logger.info("Query cancelled, stopping fetch");
    }
    logger.info("Total vector entries fetched: {}", totalFetched);
  }

  private static GeneratedRowWriter getRowWriter(ReadRecordsRequest recordsRequest) {

    GeneratedRowWriter.RowWriterBuilder builder =
        GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
    // Field: ID
    builder.withExtractor(
        COL_VECTOR_ID,
        (VarCharExtractor)
            (Object context, NullableVarCharHolder value) -> {
              value.isSet = 1;
              value.value = ((VectorData) context).getId();
            });
    // Field: Embedding
    builder.withFieldWriterFactory(
        COL_EMBEDDING_DATA,
        (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
            (Object context, int rowNum) -> {
              ((VectorData) context)
                  .getEmbedding()
                  .ifPresent(
                      embedding ->
                          BlockUtils.setComplexValue(
                              vector, rowNum, FieldResolver.DEFAULT, embedding));
              return true;
            });
    // Field: Metadata
    builder.withExtractor(
        COL_METADATA,
        (VarCharExtractor)
            (Object context, NullableVarCharHolder value) ->
                ((VectorData) context)
                    .getMetadata()
                    .ifPresent(
                        metadata -> {
                          value.isSet = 1;
                          value.value = metadata;
                        }));
    return builder.build();
  }

  private static List<String> getIds(Map<String, ValueSet> summary) {

    // todo: Support multi value only when SDK provide accurate hints.
    // https://github.com/awslabs/aws-athena-query-federation/issues/3288
    List<String> ids = new ArrayList<>();
    SortedRangeSet rangeSet = (SortedRangeSet) summary.get(COL_VECTOR_ID);
    logger.debug("Filters: {}", rangeSet);
    if (rangeSet != null) {
      for (var range : rangeSet.getOrderedRanges()) {
        if (range.isSingleValue()) {
          ids.add(range.getLow().getValue().toString());
        } else {
          logger.warn("Encounter multi value set, not eligible for Select By ID");
          return Collections.emptyList();
        }
      }
    }
    return ids;
  }
}
