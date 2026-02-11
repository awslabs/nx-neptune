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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import org.apache.arrow.util.VisibleForTesting;
//DO NOT REMOVE - this will not be _unused_ when customers go through the tutorial and uncomment
//the TODOs
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_EMBEDDING_DATA;
import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_METADATA;
import static com.amazonaws.athena.connectors.s3vector.ConnectorUtils.COL_VECTOR_ID;

/**
 * This class is part of an S3 Vector connector that enables Athena to query vector data stored in S3.
 * The README for this module (athena-example) will guide you through preparing
 * your development environment, modifying this example Metadatahandler, building, deploying, and then
 * using your new S3 vector source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with metadata about the schemas (aka databases),
 * tables, and table partitions that your S3 vector source contains. Lastly, this class tells Athena how to split up reads against
 * this S3 vector source. This gives you control over the level of performance and parallelism your source can support.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class S3VectorMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(S3VectorMetadataHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "S3 Vectors";

    private Set<String> schemas = Set.of("schema1");

    private List<String> tables = List.of(
            "table1", "table2", "table3"
    );

    public S3VectorMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
    }

    @VisibleForTesting
    protected S3VectorMetadataHandler(
        EncryptionKeyFactory keyFactory,
        SecretsManagerClient awsSecretsManager,
        AthenaClient athena,
        String spillBucket,
        String spillPrefix,
        java.util.Map<String, String> configOptions)
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
    }

    /**
     * Used to get the list of schemas (aka databases) that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: enter - " + request);

        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Used to get a paginated list of tables that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried.
     * @implNote A complete (un-paginated) list of tables should be returned if the request's pageSize is set to
     * ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logger.info("doListTables: enter - " + request);

        // todo API call to S3 vector to list out all vector indexes within the bucket.
        List<TableName> tableNameList = tables.stream()
                .map(x -> new TableName(request.getSchemaName(), x))
                .collect(Collectors.toList());

        return new ListTablesResponse(request.getCatalogName(), tableNameList, null);
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        logger.info("doGetTable: enter - " + request);

        Set<String> partitionColNames = new HashSet<>();

        SchemaBuilder tableSchemaBuilder = SchemaBuilder.newBuilder();

         tableSchemaBuilder
         .addStringField(COL_VECTOR_ID)
         .addListField(COL_EMBEDDING_DATA, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))
         .addStringField(COL_METADATA)
         .addMetadata(COL_VECTOR_ID, "Vector's unique ID.")
         .addMetadata(COL_EMBEDDING_DATA, "Array of Float32 for vector data.")
         .addMetadata(COL_METADATA, "Metadata about the vector.");

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                tableSchemaBuilder.build(),
                partitionColNames);
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter Used to write rows (partitions) into the Apache Arrow response.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @note Partitions are partially opaque to Amazon Athena in that it only understands your partition columns and
     * how to filter out partitions that do not meet the query's constraints. Any additional columns you add to the
     * partition data are ignored by Athena but passed on to calls on GetSplits.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        //NoOp since we don't support partitioning at this time.
    }

/**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     * This is an out-of-the-box (OOTB) implementation that creates a single split for the entire table.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, andpartition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     * @note A Split is a mostly opaque object to Amazon Athena. Amazon Athena will use the optional SpillLocation and
     * optional EncryptionKey for pipelined reads but all properties you set on the Split are passed to your read
     * function to help you perform the read.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        //Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);

        return new GetSplitsResponse(request.getCatalogName(), Split.newBuilder(spillLocation,
                makeEncryptionKey()).build());
    }

    /**
     * Used to describe the types of capabilities supported by a data source. An engine can use this to determine what
     * portions of the query to push down. A connector that returns any optimization will guarantee that the associated
     * predicate will be pushed down.
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details about the catalog being used.
     * @return A GetDataSourceCapabilitiesResponse object which returns a map of supported optimizations that
     * the connector is advertising to the consumer. The connector assumes all responsibility for whatever is passed here.
     */
    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        Map<String, List<OptimizationSubType>> capabilities = new HashMap<>();
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities);
    }


    public void setSchemas(Set<String> schemas) {
        this.schemas = schemas;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

}
