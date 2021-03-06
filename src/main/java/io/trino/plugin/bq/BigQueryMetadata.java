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

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.NotFoundException;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.cloud.bigquery.TableDefinition.Type.MATERIALIZED_VIEW;
import static com.google.cloud.bigquery.TableDefinition.Type.TABLE;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.bq.BigQueryType.toField;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class BigQueryMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(BigQueryMetadata.class);

    static final int NUMERIC_DATA_TYPE_PRECISION = 38;
    static final int NUMERIC_DATA_TYPE_SCALE = 9;
    static final String INFORMATION_SCHEMA = "information_schema";

    private final BigQueryClient bigQueryClient;
    private final String projectId;

    @Inject
    public BigQueryMetadata(BigQueryClient bigQueryClient, BigQueryConfig config)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
        this.projectId = requireNonNull(config, "config is null").getProjectId().orElse(bigQueryClient.getProjectId());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return Streams.stream(bigQueryClient.listDatasets(projectId))
                .map(dataset -> dataset.getDatasetId().getDataset())
                .filter(schemaName -> !schemaName.equalsIgnoreCase(INFORMATION_SCHEMA))
                .collect(toImmutableList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return listTablesWithTypes(session, schemaName, TABLE, MATERIALIZED_VIEW);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return listTablesWithTypes(session, schemaName, VIEW);
    }

    private List<SchemaTableName> listTablesWithTypes(ConnectorSession session, Optional<String> schemaName, TableDefinition.Type... types)
    {
        if (schemaName.isPresent() && schemaName.get().equalsIgnoreCase(INFORMATION_SCHEMA)) {
            return ImmutableList.of();
        }
        Set<String> schemaNames = schemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(listSchemaNames(session)));

        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String datasetId : schemaNames) {
            for (Table table : bigQueryClient.listTables(DatasetId.of(projectId, datasetId), types)) {
                tableNames.add(new SchemaTableName(datasetId, table.getTableId().getTable()));
            }
        }
        return tableNames.build();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        TableInfo tableInfo = getBigQueryTable(tableName);
        if (tableInfo == null) {
            log.debug("Table [%s.%s] was not found", tableName.getSchemaName(), tableName.getTableName());
            return null;
        }
        return BigQueryTableHandle.from(tableInfo);
    }

    @Nullable
    private TableInfo getBigQueryTable(SchemaTableName tableName)
    {
        return bigQueryClient.getTable(TableId.of(projectId, tableName.getSchemaName(), tableName.getTableName()));
    }

    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName schemaTableName)
    {
        ConnectorTableHandle table = getTableHandle(session, schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        return getTableMetadata(session, table);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableInfo table = bigQueryClient.getTable(((BigQueryTableHandle) tableHandle).getTableId());
        SchemaTableName schemaTableName = new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable());
        Schema schema = table.getDefinition().getSchema();
        List<ColumnMetadata> columns = schema == null ?
                ImmutableList.of() :
                schema.getFields().stream()
                        .map(Conversions::toColumnMetadata)
                        .collect(toImmutableList());
        return new ConnectorTableMetadata(schemaTableName, columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        List<BigQueryColumnHandle> columnHandles = getTableColumns(((BigQueryTableHandle) tableHandle).getTableId());
        return columnHandles.stream().collect(toMap(BigQueryColumnHandle::getName, identity()));
    }

    List<BigQueryColumnHandle> getTableColumns(TableId tableId)
    {
        return getTableColumns(bigQueryClient.getTable(tableId));
    }

    private List<BigQueryColumnHandle> getTableColumns(TableInfo table)
    {
        ImmutableList.Builder<BigQueryColumnHandle> columns = ImmutableList.builder();
        TableDefinition tableDefinition = table.getDefinition();
        Schema schema = tableDefinition.getSchema();
        if (schema != null) {
            for (Field field : schema.getFields()) {
                columns.add(Conversions.toColumnHandle(field));
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((BigQueryColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        SchemaTableName tableName = prefix.toSchemaTableName();
        TableInfo tableInfo = getBigQueryTable(tableName);
        return tableInfo == null ?
                ImmutableList.of() : // table does not exist
                ImmutableList.of(tableName);
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        createTable(tableMetadata);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        return createTable(tableMetadata);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns)
    {
        BigQueryTableHandle table = (BigQueryTableHandle) tableHandle;
        List<String> columnNames = columns.stream().map(column -> ((BigQueryColumnHandle) column).getName()).collect(toImmutableList());
        List<Type> columnTypes = columns.stream().map(column -> ((BigQueryColumnHandle) column).getTrinoType()).collect(toImmutableList());
        return new BigQueryInsertTableHandle(new SchemaTableName(table.getSchemaName(), table.getTableName()), columnNames, columnTypes);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        BigQueryTableHandle bigQueryTable = (BigQueryTableHandle) tableHandle;
        TableId tableId = TableId.of(projectId, bigQueryTable.getSchemaName(), bigQueryTable.getTableName());
        bigQueryClient.dropTable(tableId);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit)
    {
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        if (bigQueryTableHandle.getLimit().isPresent() && bigQueryTableHandle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        bigQueryTableHandle = bigQueryTableHandle.withLimit(limit);

        return Optional.of(new LimitApplicationResult<>(bigQueryTableHandle, false, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        List<ColumnHandle> newColumns = assignments.values().stream()
                .collect(toImmutableList());

        if (bigQueryTableHandle.getProjectedColumns().isPresent() && containSameElements(newColumns, bigQueryTableHandle.getProjectedColumns().get())) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Assignment> assignmentList = ImmutableList.builder();
        assignments.forEach((name, column) -> {
            projectedColumns.add(column);
            assignmentList.add(new Assignment(name, column, ((BigQueryColumnHandle) column).getTrinoType()));
        });

        bigQueryTableHandle = bigQueryTableHandle.withProjectedColumns(projectedColumns.build());

        return Optional.of(new ProjectionApplicationResult<>(bigQueryTableHandle, projections, assignmentList.build(), false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        TupleDomain<ColumnHandle> oldDomain = bigQueryTableHandle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        BigQueryTableHandle updatedHandle = bigQueryTableHandle.withConstraint(newDomain);

        return Optional.of(new ConstraintApplicationResult<>(updatedHandle, constraint.getSummary(), false));
    }

    private static boolean containSameElements(Iterable<? extends ColumnHandle> first, Iterable<? extends ColumnHandle> second)
    {
        return ImmutableSet.copyOf(first).equals(ImmutableSet.copyOf(second));
    }

    private BigQueryOutputTableHandle createTable(ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        ImmutableList.Builder<String> columnsNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnsTypes = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            Field field = toField(column.getName(), column.getType());
            fields.add(field);
            columnsNames.add(column.getName());
            columnsTypes.add(column.getType());
        }

        TableId tableId = TableId.of(schemaName, tableName);
        TableDefinition tableDefinition = StandardTableDefinition.of(Schema.of(fields.build()));
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        bigQueryClient.createTable(tableInfo);

        return new BigQueryOutputTableHandle(schemaTableName, columnsNames.build(), columnsTypes.build());
    }
}
