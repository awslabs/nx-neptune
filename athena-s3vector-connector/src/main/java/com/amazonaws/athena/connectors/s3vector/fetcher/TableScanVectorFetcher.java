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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.s3vector.VectorData;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.ListVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.ListVectorsResponse;

/** Fetches all vectors from an index using pagination. */
public class TableScanVectorFetcher extends AbstractVectorFetcher {
  private static final Logger logger =
      LoggerFactory.getLogger(TableScanVectorFetcher.class);

  private String nextToken;
  private boolean hasMore;
  private int currentPage;
  private int totalFetched;

  /** Constructor for TableScanVectorFetcher. */
  public TableScanVectorFetcher(
      S3VectorsClient vectorsClient, ReadRecordsRequest recordsRequest) {
    super(vectorsClient, recordsRequest);
    this.nextToken = null;
    this.hasMore = true;
    this.currentPage = 0;
    this.totalFetched = 0;
  }

  @Override
  public boolean hasNext() {
    return hasMore && (limit <= 0 || totalFetched < limit);
  }

  @Override
  public List<VectorData> next() {
    var requestBuilder =
        ListVectorsRequest.builder()
            .vectorBucketName(bucketName)
            .indexName(indexName)
            .returnData(fetchEmbedding)
            .returnMetadata(fetchMetadata);

    if (nextToken != null) {
      requestBuilder.nextToken(nextToken);
    }

    ListVectorsResponse response = vectorsClient.listVectors(requestBuilder.build());
    currentPage++;

    List<VectorData> results =
        response.vectors().stream()
            .map(
                item ->
                    new VectorData(
                        item.key(),
                        fetchEmbedding ? item.data().float32() : null,
                        fetchMetadata ? item.metadata().toString() : null))
            .collect(Collectors.toList());

    if (limit > 0 && totalFetched + results.size() > limit) {
      results = results.subList(0, (int) (limit - totalFetched));
      hasMore = false;
    }

    totalFetched += results.size();
    logger.debug(
        "Fetched page {} with {} vectors (total: {}, limit: {})",
        currentPage,
        results.size(),
        totalFetched,
        limit);

    nextToken = response.nextToken();
    if (nextToken == null) {
      hasMore = false;
    }

    return results;
  }
}
