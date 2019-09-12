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

import com.google.common.collect.Lists;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumns;

import java.util.List;
import java.util.Map;

/**
 */
public class Druids {
    private Druids() {
        throw new AssertionError();
    }

    public static SelectQueryBuilder newSelectQueryBuilder() {
        return new SelectQueryBuilder();
    }

    /**
     * A Builder for SelectQuery.
     * <p/>
     * Required: dataSource(), intervals() must be called before build()
     * <p/>
     * Usage example:
     * <pre><code>
     *   SelectQuery query = new SelectQueryBuilder()
     *                                  .dataSource("Example")
     *                                  .interval("2010/2013")
     *                                  .build();
     * </code></pre>
     *
     * @see SelectQuery
     */
    public static class SelectQueryBuilder {
        private DataSource dataSource;
        private QuerySegmentSpec querySegmentSpec;
        private boolean descending;
        private Map<String, Object> context;
        private DimFilter dimFilter;
        private Granularity granularity;
        private List<DimensionSpec> dimensions;
        private List<String> metrics;
        private VirtualColumns virtualColumns;
        private PagingSpec pagingSpec;

        public SelectQueryBuilder() {
            dataSource = null;
            querySegmentSpec = null;
            descending = false;
            context = null;
            dimFilter = null;
            granularity = Granularities.ALL;
            dimensions = Lists.newArrayList();
            metrics = Lists.newArrayList();
            virtualColumns = null;
            pagingSpec = null;
        }

        public static SelectQueryBuilder copy(SelectQuery query) {
            return new SelectQueryBuilder()
                    .dataSource(query.getDataSource())
                    .intervals(query.getQuerySegmentSpec())
                    .descending(query.isDescending())
                    .filters(query.getFilter())
                    .granularity(query.getGranularity())
                    .dimensionSpecs(query.getDimensions())
                    .metrics(query.getMetrics())
                    .virtualColumns(query.getVirtualColumns())
                    .pagingSpec(query.getPagingSpec())
                    .context(query.getContext());
        }

        public SelectQuery build() {
            return new SelectQuery(
                    dataSource,
                    querySegmentSpec,
                    descending,
                    dimFilter,
                    granularity,
                    dimensions,
                    metrics,
                    virtualColumns,
                    pagingSpec,
                    context
            );
        }

        public SelectQueryBuilder dataSource(String ds) {
            dataSource = new TableDataSource(ds);
            return this;
        }

        public SelectQueryBuilder dataSource(DataSource ds) {
            dataSource = ds;
            return this;
        }

        public SelectQueryBuilder intervals(QuerySegmentSpec q) {
            querySegmentSpec = q;
            return this;
        }

        public SelectQueryBuilder intervals(String s) {
            querySegmentSpec = new LegacySegmentSpec(s);
            return this;
        }

        public SelectQueryBuilder descending(boolean descending) {
            this.descending = descending;
            return this;
        }

        public SelectQueryBuilder context(Map<String, Object> c) {
            context = c;
            return this;
        }

        public SelectQueryBuilder filters(DimFilter f) {
            dimFilter = f;
            return this;
        }

        public SelectQueryBuilder granularity(Granularity g) {
            granularity = g;
            return this;
        }

        public SelectQueryBuilder dimensionSpecs(List<DimensionSpec> d) {
            dimensions = d;
            return this;
        }

        public SelectQueryBuilder dimensions(List<String> d) {
            dimensions = DefaultDimensionSpec.toSpec(d);
            return this;
        }

        public SelectQueryBuilder metrics(List<String> m) {
            metrics = m;
            return this;
        }

        public SelectQueryBuilder virtualColumns(VirtualColumns vcs) {
            virtualColumns = vcs;
            return this;
        }

        public SelectQueryBuilder pagingSpec(PagingSpec p) {
            pagingSpec = p;
            return this;
        }
    }
}
