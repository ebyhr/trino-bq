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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class BigQueryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final BigQueryClient bigQueryClient;

    @Inject
    public BigQueryPageSourceProvider(BigQueryClient bigQueryClient)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        BigQueryTableHandle tableHandle = (BigQueryTableHandle) table;
        BigQuerySplit bigQuerySplit = (BigQuerySplit) split;

        ImmutableList.Builder<BigQueryColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : requireNonNull(columns, "columns is null")) {
            handles.add((BigQueryColumnHandle) handle);
        }

        return new BigQueryPageSource(bigQueryClient, bigQuerySplit, tableHandle, handles.build());
    }
}
