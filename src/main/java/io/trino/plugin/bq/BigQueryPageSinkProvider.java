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

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

public class BigQueryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final BigQueryClient bigQueryClient;

    @Inject
    public BigQueryPageSinkProvider(BigQueryClient bigQueryClient)
    {
        this.bigQueryClient = bigQueryClient;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        BigQueryOutputTableHandle handle = (BigQueryOutputTableHandle) outputTableHandle;
        return new BigQueryPageSink(bigQueryClient, handle.getSchemaTableName(), handle.getColumnNames(), handle.getColumnTypes());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        BigQueryInsertTableHandle handle = (BigQueryInsertTableHandle) insertTableHandle;
        return new BigQueryPageSink(bigQueryClient, handle.getSchemaTableName(), handle.getColumnNames(), handle.getColumnTypes());
    }
}
