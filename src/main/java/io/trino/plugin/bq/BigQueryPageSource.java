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

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.bq.BigQueryMetadata.NUMERIC_DATA_TYPE_SCALE;
import static io.trino.plugin.bq.BigQueryType.toTrinoTimestamp;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class BigQueryPageSource
        implements ConnectorPageSource
{
    private final List<Type> columnTypes;
    private final PageBuilder pageBuilder;
    private final TableResult tableResult;
    private long count;
    private boolean finished;

    public BigQueryPageSource(
            BigQueryClient bigQueryClient,
            BigQuerySplit split,
            BigQueryTableHandle table,
            List<BigQueryColumnHandle> columns)
    {
        List<String> columnNames = columns.stream().map(BigQueryColumnHandle::getName).collect(toList());
        this.columnTypes = columns.stream().map(BigQueryColumnHandle::getTrinoType).collect(toImmutableList());
        this.pageBuilder = new PageBuilder(columnTypes);
        this.tableResult = bigQueryClient.query(table.getTableId(), columnNames, split.getAdditionalPredicate());
    }

    @Override
    public long getCompletedBytes()
    {
        return count;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        verify(pageBuilder.isEmpty());
        for (FieldValueList record : tableResult.iterateAll()) {
            count++;
            pageBuilder.declarePosition();
            for (int column = 0; column < columnTypes.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                appendTo(columnTypes.get(column), record.get(column), output);
            }
        }
        finished = true;

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private void appendTo(Type type, FieldValue value, BlockBuilder output)
    {
        if (value == null || value.isNull()) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, value.getBooleanValue());
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    type.writeLong(output, value.getLongValue());
                }
                else if (type.equals(INTEGER)) {
                    type.writeLong(output, value.getLongValue());
                }
                else if (type.equals(DATE)) {
                    type.writeLong(output, LocalDate.parse(value.getStringValue()).toEpochDay());
                }
                else if (type.equals(TIMESTAMP_MILLIS)) {
                    type.writeLong(output, toTrinoTimestamp((value.getStringValue())));
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, value.getDoubleValue());
            }
            else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            }
            else if (javaType == Block.class) {
                writeBlock(output, type, value);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ignore) {
            // returns null instead of raising exception
            output.appendNull();
        }
    }

    private static void writeSlice(BlockBuilder output, Type type, FieldValue value)
    {
        if (type instanceof VarcharType) {
            type.writeSlice(output, utf8Slice(value.getStringValue()));
        }
        else if (type instanceof DecimalType) {
            BigDecimal bdValue = value.getNumericValue();
            type.writeSlice(output, Decimals.encodeScaledValue(bdValue, NUMERIC_DATA_TYPE_SCALE));
        }
        else if (type instanceof VarbinaryType) {
            type.writeSlice(output, Slices.wrappedBuffer(value.getBytesValue()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private void writeBlock(BlockBuilder output, Type type, FieldValue value)
    {
        if (type instanceof ArrayType) {
            BlockBuilder builder = output.beginBlockEntry();

            for (FieldValue element : value.getRepeatedValue()) {
                appendTo(type.getTypeParameters().get(0), element, builder);
            }

            output.closeEntry();
            return;
        }
        if (type instanceof RowType) {
            FieldValueList record = value.getRecordValue();
            BlockBuilder builder = output.beginBlockEntry();

            List<String> fieldNames = new ArrayList<>();
            for (int i = 0; i < type.getTypeSignature().getParameters().size(); i++) {
                TypeSignatureParameter parameter = type.getTypeSignature().getParameters().get(i);
                fieldNames.add(parameter.getNamedTypeSignature().getName().orElse("field" + i));
            }
            checkState(fieldNames.size() == type.getTypeParameters().size(), "fieldName doesn't match with type size : %s", type);
            for (int index = 0; index < type.getTypeParameters().size(); index++) {
                appendTo(type.getTypeParameters().get(index), record.get(fieldNames.get(index)), builder);
            }
            output.closeEntry();
            return;
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() {}
}
