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

import java.util.Objects;

public class BatchPartition {
    private final String bucketName;
    private final String indexName;

    public BatchPartition(String bucketName, String indexName) {
        this.bucketName = bucketName;
        this.indexName = indexName;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchPartition that = (BatchPartition) o;
        return Objects.equals(bucketName, that.bucketName) && Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketName, indexName);
    }

    @Override
    public String toString() {
        return "BatchPartition{" +
                "bucketName='" + bucketName + '\'' +
                ", indexName='" + indexName + '\'' +
                '}';
    }
}
