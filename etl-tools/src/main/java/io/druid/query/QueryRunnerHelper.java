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

package io.druid.query;

import com.google.common.base.Preconditions;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.filter.Filter;
import io.druid.segment.Cursor;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 */
public class QueryRunnerHelper {

    public static <T> Sequence<Result<T>> makeCursorBasedQuery(
            final StorageAdapter adapter,
            List<Interval> queryIntervals,
            Filter filter,
            VirtualColumns virtualColumns,
            boolean descending,
            Granularity granularity,
            final Function<Cursor, Result<T>> mapFn
    ) {
        Preconditions.checkArgument(
                queryIntervals.size() == 1, "Can only handle a single interval, got[%s]", queryIntervals
        );

        return Sequences.filter(
                Sequences.map(
                        adapter.makeCursors(filter, queryIntervals.get(0), virtualColumns, granularity, descending, null),
                        mapFn::apply
                ),
                Objects::nonNull
        );
    }
}
