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
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;

import java.util.List;

/**
 * Abstract base class for vector fetchers with common attributes.
 */
public abstract class AbstractVectorFetcher
{
    protected final S3VectorsClient vectorsClient;
    protected final String bucketName;
    protected final String indexName;
    protected final boolean fetchEmbedding;
    protected final boolean fetchMetadata;

    protected AbstractVectorFetcher(S3VectorsClient vectorsClient, String bucketName, String indexName,
                                     boolean fetchEmbedding, boolean fetchMetadata)
    {
        this.vectorsClient = vectorsClient;
        this.bucketName = bucketName;
        this.indexName = indexName;
        this.fetchEmbedding = fetchEmbedding;
        this.fetchMetadata = fetchMetadata;
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
