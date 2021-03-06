/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *http://www.apache.org/licenses/LICENSE-2.0
 *Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.FloatColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedColumnarFloatsSupplier;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 */
public class FloatGenericColumnPartSerde implements ColumnPartSerde {
    private final ByteOrder byteOrder;
    private final Serializer serializer;

    private FloatGenericColumnPartSerde(ByteOrder byteOrder, Serializer serializer) {
        this.byteOrder = byteOrder;
        this.serializer = serializer;
    }

    @JsonCreator
    public static FloatGenericColumnPartSerde createDeserializer(
            @JsonProperty("byteOrder") ByteOrder byteOrder
    ) {
        return new FloatGenericColumnPartSerde(byteOrder, null);
    }

    public static SerializerBuilder serializerBuilder() {
        return new SerializerBuilder();
    }

    @JsonProperty
    public ByteOrder getByteOrder() {
        return byteOrder;
    }

    @Override
    public Serializer getSerializer() {
        return serializer;
    }

    @Override
    public Deserializer getDeserializer() {
        return new Deserializer() {
            @Override
            public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig) {
                final CompressedColumnarFloatsSupplier column = CompressedColumnarFloatsSupplier.fromByteBuffer(
                        buffer,
                        byteOrder
                );
                builder.setType(ValueType.FLOAT)
                        .setHasMultipleValues(false)
                        .setGenericColumn(new FloatGenericColumnSupplier(column));
            }
        };
    }

    public static class SerializerBuilder {
        private ByteOrder byteOrder = null;
        private FloatColumnSerializer delegate = null;

        public SerializerBuilder withByteOrder(final ByteOrder byteOrder) {
            this.byteOrder = byteOrder;
            return this;
        }

        public SerializerBuilder withDelegate(final FloatColumnSerializer delegate) {
            this.delegate = delegate;
            return this;
        }

        public FloatGenericColumnPartSerde build() {
            return new FloatGenericColumnPartSerde(byteOrder, delegate);
        }
    }
}
