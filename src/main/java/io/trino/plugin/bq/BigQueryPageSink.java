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

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class BigQueryPageSink
        implements ConnectorPageSink
{
    private final BigQueryClient bigQueryClient;
    private final SchemaTableName schemaTableName;
    private final List<BigQueryColumnHandle> columns;

    public BigQueryPageSink(BigQueryClient bigQueryClient, SchemaTableName schemaTableName, List<BigQueryColumnHandle> columns)
    {
        this.bigQueryClient = bigQueryClient;
        this.schemaTableName = schemaTableName;
        this.columns = columns;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        InsertAllRequest.Builder batch = InsertAllRequest.newBuilder(bigQueryClient.getTableId(schemaTableName));

        for (int position = 0; position < page.getPositionCount(); position++) {
            Map<String, Object> row = new HashMap<>();

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                BigQueryColumnHandle column = columns.get(channel);
                row.put(column.getName(), getObjectValue(column.getTrinoType(), page.getBlock(channel), position));
            }
            batch.addRow(row);
        }

        bigQueryClient.insert(batch.build());
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }

    private Object getObjectValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (type.equals(BooleanType.BOOLEAN)) {
            return type.getBoolean(block, position);
        }
        if (type.equals(BigintType.BIGINT)) {
            return type.getLong(block, position);
        }
        if (type.equals(IntegerType.INTEGER)) {
            return toIntExact(type.getLong(block, position));
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return Shorts.checkedCast(type.getLong(block, position));
        }
        if (type.equals(TinyintType.TINYINT)) {
            return SignedBytes.checkedCast(type.getLong(block, position));
        }
        if (type.equals(RealType.REAL)) {
            return intBitsToFloat(toIntExact(type.getLong(block, position)));
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return type.getDouble(block, position);
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof CharType) {
            return padSpaces(type.getSlice(block, position), ((CharType) type)).toStringUtf8();
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return type.getSlice(block, position).getBytes();
        }
        if (type.equals(DateType.DATE)) {
            long days = type.getLong(block, position);
            return new Date(TimeUnit.DAYS.toMillis(days));
        }
        if (type.equals(TimeType.TIME)) {
            long picos = type.getLong(block, position);
            return new Date(roundDiv(picos, PICOSECONDS_PER_MILLISECOND));
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            long millisUtc = floorDiv(type.getLong(block, position), MICROSECONDS_PER_MILLISECOND);
            return new Date(millisUtc);
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)) {
            long millisUtc = unpackMillisUtc(type.getLong(block, position));
            return new Date(millisUtc);
        }
        if (type instanceof DecimalType) {
            return readBigDecimal((DecimalType) type, block, position);
        }
        if (type instanceof ArrayType) {
            Type elementType = type.getTypeParameters().get(0);

            Block arrayBlock = block.getObject(position, Block.class);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = getObjectValue(elementType, arrayBlock, i);
                list.add(element);
            }

            return unmodifiableList(list);
        }
        if (type instanceof MapType) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            Block mapBlock = block.getObject(position, Block.class);

            // map type is converted into list of fixed keys document
            List<Object> values = new ArrayList<>(mapBlock.getPositionCount() / 2);
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                Map<String, Object> mapValue = new HashMap<>();
                mapValue.put("key", getObjectValue(keyType, mapBlock, i));
                mapValue.put("value", getObjectValue(valueType, mapBlock, i + 1));
                values.add(mapValue);
            }

            return unmodifiableList(values);
        }
        if (type instanceof RowType) {
            Block rowBlock = block.getObject(position, Block.class);

            List<Type> fieldTypes = type.getTypeParameters();
            if (fieldTypes.size() != rowBlock.getPositionCount()) {
                throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");
            }

            Map<String, Object> rowValue = new HashMap<>();
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                rowValue.put(
                        type.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName().orElse("field" + i),
                        getObjectValue(fieldTypes.get(i), rowBlock, i));
            }
            return unmodifiableMap(rowValue);
        }

        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }
}
