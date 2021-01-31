/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.bq;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.spi.connector.SchemaTableName;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class BigQueryClient
{
    private static final Logger log = Logger.get(BigQueryClient.class);

    private final BigQuery bigQuery;
    private final Optional<String> projectId;

    public BigQueryClient(BigQuery bigQuery, Optional<String> projectId)
    {
        this.bigQuery = bigQuery;
        this.projectId = projectId;
    }

    @Nullable
    public TableInfo getTable(TableId tableId)
    {
        return bigQuery.getTable(tableId);
    }

    public TableId getTableId(SchemaTableName schemaTableName)
    {
        return TableId.of(getProjectId(), schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    public String getProjectId()
    {
        return projectId.orElse(bigQuery.getOptions().getProjectId());
    }

    public Iterable<Dataset> listDatasets(String projectId)
    {
        return bigQuery.listDatasets(projectId).iterateAll();
    }

    public Iterable<Table> listTables(DatasetId datasetId, TableDefinition.Type... types)
    {
        Set<TableDefinition.Type> allowedTypes = ImmutableSet.copyOf(types);
        Iterable<Table> allTables = bigQuery.listTables(datasetId).iterateAll();
        return StreamSupport.stream(allTables.spliterator(), false)
                .filter(table -> allowedTypes.contains(table.getDefinition().getType()))
                .collect(toImmutableList());
    }

    public void createTable(TableInfo tableInfo)
    {
        bigQuery.create(tableInfo);
    }

    public void insert(InsertAllRequest insertAllRequest)
    {
        InsertAllResponse response = bigQuery.insertAll(insertAllRequest);
        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> errors : response.getInsertErrors().entrySet()) {
                log.error("Failed to insert row: %s, error: %s", errors.getKey(), errors.getValue());
            }
        }
    }

    public void dropTable(TableId tableId)
    {
        bigQuery.delete(tableId);
    }

    public TableResult query(TableId table, List<String> requiredColumns, Optional<String> filter)
    {
        String sql = selectSql(table, requiredColumns, filter);
        log.debug("Execute query: %s", sql);
        try {
            return bigQuery.query(QueryJobConfiguration.of(sql));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryException(BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the query [%s]", sql), e);
        }
    }

    public String selectSql(TableId table, List<String> requiredColumns, Optional<String> filter)
    {
        String columns = requiredColumns.isEmpty() ? "*" :
                requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

        return selectSql(table, columns, filter);
    }

    private String selectSql(TableId table, String formattedColumns, Optional<String> filter)
    {
        String tableName = fullTableName(table);
        String query = format("SELECT %s FROM `%s`", formattedColumns, tableName);
        if (filter.isEmpty()) {
            return query;
        }
        return query + " WHERE " + filter.get();
    }

    private String fullTableName(TableId tableId)
    {
        return format("%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }
}
