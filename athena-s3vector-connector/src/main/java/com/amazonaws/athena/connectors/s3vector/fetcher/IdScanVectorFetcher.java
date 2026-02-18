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

import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_VECTOR_ID;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.s3vector.VectorData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.GetVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.GetVectorsResponse;

/** Fetches vectors by specific IDs in batches. */
public class IdScanVectorFetcher extends AbstractVectorFetcher {
  private static final Logger logger =
      LoggerFactory.getLogger(IdScanVectorFetcher.class);
  // Default server limit is 100.
  private static final int BATCH_SIZE = 80;

  private final List<String> allIds;
  private int currentIndex;
  private int totalFetched;

  /** Constructor for IdScanVectorFetcher. */
  public IdScanVectorFetcher(
      S3VectorsClient vectorsClient, ReadRecordsRequest recordsRequest, List<String> ids) {
    super(vectorsClient, recordsRequest);
    this.allIds = ids;
    this.currentIndex = 0;
    this.totalFetched = 0;
    logger.info("Executing ID scan operations with size: {}", allIds.size());
  }

  @Override
  public boolean hasNext() {
    return currentIndex < allIds.size() && (limit <= 0 || totalFetched < limit);
  }

  @Override
  public List<VectorData> next() {
    int endIndex = Math.min(currentIndex + BATCH_SIZE, allIds.size());
    if (limit > 0) {
      endIndex = Math.min(endIndex, currentIndex + (int) (limit - totalFetched));
    }
    List<String> batchIds = allIds.subList(currentIndex, endIndex);

    var request =
        GetVectorsRequest.builder()
            .vectorBucketName(bucketName)
            .indexName(indexName)
            .keys(batchIds)
            .returnData(fetchEmbedding)
            .returnMetadata(fetchMetadata)
            .build();

    GetVectorsResponse response = vectorsClient.getVectors(request);

    List<VectorData> results =
        response.vectors().stream()
            .map(
                item ->
                    new VectorData(
                        item.key(),
                        fetchEmbedding ? item.data().float32() : null,
                        fetchMetadata ? item.metadata().toString() : null))
            .collect(Collectors.toList());

    currentIndex = endIndex;
    totalFetched += results.size();
    logger.debug(
        "Fetched {} vectors by ID (batch {}-{}, total: {}, limit: {})",
        results.size(),
        currentIndex - results.size(),
        currentIndex,
        totalFetched,
        limit);

    return results;
  }
}
