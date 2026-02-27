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
