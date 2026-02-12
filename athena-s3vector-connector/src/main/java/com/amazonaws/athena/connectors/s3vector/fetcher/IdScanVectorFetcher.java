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

import com.amazonaws.athena.connectors.s3vector.VectorData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.GetVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.GetVectorsResponse;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Fetches vectors by specific IDs in batches.
 */
public class IdScanVectorFetcher extends AbstractVectorFetcher
{
    private static final Logger logger = LoggerFactory.getLogger(IdScanVectorFetcher.class);
    private static final int BATCH_SIZE = 300;

    private final List<String> allIds;
    private int currentIndex;

    public IdScanVectorFetcher(S3VectorsClient vectorsClient, String bucketName, String indexName,
                                List<String> ids, boolean fetchEmbedding, boolean fetchMetadata)
    {
        super(vectorsClient, bucketName, indexName, fetchEmbedding, fetchMetadata);
        this.allIds = ids;
        this.currentIndex = 0;
    }

    @Override
    public boolean hasNext()
    {
        return currentIndex < allIds.size();
    }

    @Override
    public List<VectorData> next()
    {
        int endIndex = Math.min(currentIndex + BATCH_SIZE, allIds.size());
        List<String> batchIds = allIds.subList(currentIndex, endIndex);

        var request = GetVectorsRequest.builder()
                .vectorBucketName(bucketName)
                .indexName(indexName)
                .keys(batchIds)
                .returnData(fetchEmbedding)
                .returnMetadata(fetchMetadata)
                .build();

        GetVectorsResponse response = vectorsClient.getVectors(request);
        logger.debug("Fetched {} vectors by ID (batch {}-{})", response.vectors().size(), currentIndex, endIndex);

        currentIndex = endIndex;

        return response.vectors().stream()
                .map(item ->
                    new VectorData(item.key(),
                            fetchEmbedding ? item.data().float32() : null,
                            fetchMetadata ? item.metadata().toString() : null
                ))
                .collect(Collectors.toList());
    }
}
