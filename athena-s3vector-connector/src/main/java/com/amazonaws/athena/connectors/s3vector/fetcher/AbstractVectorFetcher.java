/*-
 * #%L
 * athena-s3vector-connector
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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

package com.amazonaws.athena.connectors.s3vector.fetcher;

import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_EMBEDDING_DATA;
import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_METADATA;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.s3vector.VectorData;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;

/** Abstract base class for vector fetchers with common attributes. */
public abstract class AbstractVectorFetcher {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractVectorFetcher.class);
  protected final S3VectorsClient vectorsClient;
  protected final String bucketName;
  protected final String indexName;
  protected final boolean fetchEmbedding;
  protected final boolean fetchMetadata;
  protected final long limit;

  protected AbstractVectorFetcher(
      S3VectorsClient vectorsClient, ReadRecordsRequest recordsRequest) {
    final Split split = recordsRequest.getSplit();
    final TableName tableName = recordsRequest.getTableName();
    final Set<String> columnNamesSst =
        recordsRequest.getSchema().getFields().stream()
            .map(Field::getName)
            .filter(c -> !split.getProperties().containsKey(c))
            .collect(Collectors.toSet());
    final Constraints constraints = recordsRequest.getConstraints();

    this.vectorsClient = vectorsClient;
    this.bucketName = tableName.getSchemaName();
    this.indexName = tableName.getTableName();
    this.fetchEmbedding = columnNamesSst.contains(COL_EMBEDDING_DATA);
    this.fetchMetadata = columnNamesSst.contains(COL_METADATA);

    this.limit = constraints.getLimit();

    logger.debug("Request: {}", recordsRequest);
    logger.debug("Summary: {}", recordsRequest.getConstraints().getSummary());

    logger.info(
        "Execute fetch request with config: [fetchEmbedding: {}, fetchMetadata: {}, limit: {}]",
        columnNamesSst.contains(COL_EMBEDDING_DATA),
        columnNamesSst.contains(COL_METADATA),
        limit);
  }

  /**
   * Checks if there are more vectors to fetch.
   *
   * @return true if more vectors are available
   */
  public abstract boolean hasNext();

  /**
   * Fetches the next batch of vectors.
   *
   * @return List of VectorData for the next batch
   */
  public abstract List<VectorData> next();
}
