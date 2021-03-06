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

package io.druid.query.aggregation.first;

import io.druid.collections.SerializablePair;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.BaseFloatColumnValueSelector;
import io.druid.segment.BaseLongColumnValueSelector;

import java.nio.ByteBuffer;

public class FloatFirstBufferAggregator implements BufferAggregator {
    private final BaseLongColumnValueSelector timeSelector;
    private final BaseFloatColumnValueSelector valueSelector;

    public FloatFirstBufferAggregator(
            BaseLongColumnValueSelector timeSelector,
            BaseFloatColumnValueSelector valueSelector
    ) {
        this.timeSelector = timeSelector;
        this.valueSelector = valueSelector;
    }

    @Override
    public void init(ByteBuffer buf, int position) {
        buf.putLong(position, Long.MAX_VALUE);
        buf.putFloat(position + Long.BYTES, 0);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position) {
        long time = timeSelector.getLong();
        long firstTime = buf.getLong(position);
        if (time < firstTime) {
            buf.putLong(position, time);
            buf.putFloat(position + Long.BYTES, valueSelector.getFloat());
        }
    }

    @Override
    public Object get(ByteBuffer buf, int position) {
        return new SerializablePair<>(buf.getLong(position), buf.getFloat(position + Long.BYTES));
    }

    @Override
    public float getFloat(ByteBuffer buf, int position) {
        return buf.getFloat(position + Long.BYTES);
    }

    @Override
    public long getLong(ByteBuffer buf, int position) {
        return (long) buf.getFloat(position + Long.BYTES);
    }

    @Override
    public double getDouble(ByteBuffer buf, int position) {
        return (double) buf.getFloat(position + Long.BYTES);
    }

    @Override
    public void close() {
        // no resources to cleanup
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
        inspector.visit("timeSelector", timeSelector);
        inspector.visit("valueSelector", valueSelector);
    }
}
