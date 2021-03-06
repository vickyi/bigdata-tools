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

package io.druid.segment.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.query.BitmapResultFactory;
import io.druid.query.Query;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BooleanFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.Filter;
import io.druid.segment.ColumnSelector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 */
public class Filters {
    public static final List<ValueType> FILTERABLE_TYPES = ImmutableList.of(
            ValueType.STRING,
            ValueType.LONG,
            ValueType.FLOAT,
            ValueType.DOUBLE
    );
    private static final String CTX_KEY_USE_FILTER_CNF = "useFilterCNF";

    /**
     * Convert a list of DimFilters to a list of Filters.
     *
     * @param dimFilters list of DimFilters, should all be non-null
     * @return list of Filters
     */
    public static List<Filter> toFilters(List<DimFilter> dimFilters) {
        return ImmutableList.copyOf(
                FunctionalIterable
                        .create(dimFilters)
                        .transform(
                                input -> input.toFilter()
                        )
        );
    }

    /**
     * Convert a DimFilter to a Filter.
     *
     * @param dimFilter dimFilter
     * @return converted filter, or null if input was null
     */
    public static Filter toFilter(DimFilter dimFilter) {
        return dimFilter == null ? null : dimFilter.toFilter();
    }


    public static ImmutableBitmap allFalse(final BitmapIndexSelector selector) {
        return selector.getBitmapFactory().makeEmptyImmutableBitmap();
    }

    public static ImmutableBitmap allTrue(final BitmapIndexSelector selector) {
        return selector.getBitmapFactory()
                .complement(selector.getBitmapFactory().makeEmptyImmutableBitmap(), selector.getNumRows());
    }

    /**
     * Transform an iterable of indexes of bitmaps to an iterable of bitmaps
     *
     * @param indexes     indexes of bitmaps
     * @param bitmapIndex an object to retrieve bitmaps using indexes
     * @return an iterable of bitmaps
     */
    static Iterable<ImmutableBitmap> bitmapsFromIndexes(final IntIterable indexes, final BitmapIndex bitmapIndex) {
        // Do not use Iterables.transform() to avoid boxing/unboxing integers.
        return new Iterable<ImmutableBitmap>() {
            @Override
            public Iterator<ImmutableBitmap> iterator() {
                final IntIterator iterator = indexes.iterator();

                return new Iterator<ImmutableBitmap>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public ImmutableBitmap next() {
                        return bitmapIndex.getBitmap(iterator.nextInt());
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Return the union of bitmaps for all values matching a particular predicate.
     *
     * @param dimension           dimension to look at
     * @param selector            bitmap selector
     * @param bitmapResultFactory
     * @param predicate           predicate to use
     * @return bitmap of matching rows
     * @see #estimateSelectivity(String, BitmapIndexSelector, Predicate)
     */
    public static <T> T matchPredicate(
            final String dimension,
            final BitmapIndexSelector selector,
            BitmapResultFactory<T> bitmapResultFactory,
            final Predicate<String> predicate
    ) {
        return bitmapResultFactory.unionDimensionValueBitmaps(matchPredicateNoUnion(dimension, selector, predicate));
    }

    /**
     * Return an iterable of bitmaps for all values matching a particular predicate. Unioning these bitmaps
     * yields the same result that {@link #matchPredicate(String, BitmapIndexSelector, BitmapResultFactory, Predicate)}
     * would have returned.
     *
     * @param dimension dimension to look at
     * @param selector  bitmap selector
     * @param predicate predicate to use
     * @return iterable of bitmaps of matching rows
     */
    public static Iterable<ImmutableBitmap> matchPredicateNoUnion(
            final String dimension,
            final BitmapIndexSelector selector,
            final Predicate<String> predicate
    ) {
        Preconditions.checkNotNull(dimension, "dimension");
        Preconditions.checkNotNull(selector, "selector");
        Preconditions.checkNotNull(predicate, "predicate");

        // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
        final Indexed<String> dimValues = selector.getDimensionValues(dimension);
        if (dimValues == null || dimValues.size() == 0) {
            return ImmutableList.of(predicate.apply(null) ? allTrue(selector) : allFalse(selector));
        }

        // Apply predicate to all dimension values and union the matching bitmaps
        final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
        return makePredicateQualifyingBitmapIterable(bitmapIndex, predicate, dimValues);
    }

    private static Iterable<ImmutableBitmap> makePredicateQualifyingBitmapIterable(
            final BitmapIndex bitmapIndex,
            final Predicate<String> predicate,
            final Indexed<String> dimValues
    ) {
        return bitmapsFromIndexes(makePredicateQualifyingIndexIterable(bitmapIndex, predicate, dimValues), bitmapIndex);
    }

    private static IntIterable makePredicateQualifyingIndexIterable(
            final BitmapIndex bitmapIndex,
            final Predicate<String> predicate,
            final Indexed<String> dimValues
    ) {
        return new IntIterable() {
            @Override
            public IntIterator iterator() {
                return new IntIterator() {
                    private final int bitmapIndexCardinality = bitmapIndex.getCardinality();
                    private int nextIndex = 0;
                    private int found = -1;

                    {
                        found = findNextIndex();
                    }

                    private int findNextIndex() {
                        while (nextIndex < bitmapIndexCardinality && !predicate.apply(dimValues.get(nextIndex))) {
                            nextIndex++;
                        }

                        if (nextIndex < bitmapIndexCardinality) {
                            return nextIndex++;
                        } else {
                            return -1;
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return found != -1;
                    }

                    @Override
                    public int nextInt() {
                        int foundIndex = this.found;
                        if (foundIndex == -1) {
                            throw new NoSuchElementException();
                        }
                        this.found = findNextIndex();
                        return foundIndex;
                    }
                };
            }
        };
    }

    static boolean supportsSelectivityEstimation(
            Filter filter,
            String dimension,
            ColumnSelector columnSelector,
            BitmapIndexSelector indexSelector
    ) {
        if (filter.supportsBitmapIndex(indexSelector)) {
            final Column column = columnSelector.getColumn(dimension);
            if (column != null) {
                return !column.getCapabilities().hasMultipleValues();
            }
        }
        return false;
    }

    public static Filter convertToCNFFromQueryContext(Query query, Filter filter) {
        if (filter == null) {
            return null;
        }
        boolean useCNF = query.getContextBoolean(CTX_KEY_USE_FILTER_CNF, false);
        return useCNF ? convertToCNF(filter) : filter;
    }

    public static Filter convertToCNF(Filter current) {
        current = pushDownNot(current);
        current = flatten(current);
        current = convertToCNFInternal(current);
        current = flatten(current);
        return current;
    }

    // CNF conversion functions were adapted from Apache Hive, see:
    // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
    private static Filter pushDownNot(Filter current) {
        if (current instanceof NotFilter) {
            Filter child = ((NotFilter) current).getBaseFilter();
            if (child instanceof NotFilter) {
                return pushDownNot(((NotFilter) child).getBaseFilter());
            }
            if (child instanceof AndFilter) {
                List<Filter> children = Lists.newArrayList();
                for (Filter grandChild : ((AndFilter) child).getFilters()) {
                    children.add(pushDownNot(new NotFilter(grandChild)));
                }
                return new OrFilter(children);
            }
            if (child instanceof OrFilter) {
                List<Filter> children = Lists.newArrayList();
                for (Filter grandChild : ((OrFilter) child).getFilters()) {
                    children.add(pushDownNot(new NotFilter(grandChild)));
                }
                return new AndFilter(children);
            }
        }


        if (current instanceof AndFilter) {
            List<Filter> children = Lists.newArrayList();
            for (Filter child : ((AndFilter) current).getFilters()) {
                children.add(pushDownNot(child));
            }
            return new AndFilter(children);
        }


        if (current instanceof OrFilter) {
            List<Filter> children = Lists.newArrayList();
            for (Filter child : ((OrFilter) current).getFilters()) {
                children.add(pushDownNot(child));
            }
            return new OrFilter(children);
        }
        return current;
    }

    // CNF conversion functions were adapted from Apache Hive, see:
    // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
    private static Filter convertToCNFInternal(Filter current) {
        if (current instanceof NotFilter) {
            return new NotFilter(convertToCNFInternal(((NotFilter) current).getBaseFilter()));
        }
        if (current instanceof AndFilter) {
            List<Filter> children = Lists.newArrayList();
            for (Filter child : ((AndFilter) current).getFilters()) {
                children.add(convertToCNFInternal(child));
            }
            return new AndFilter(children);
        }
        if (current instanceof OrFilter) {
            // a list of leaves that weren't under AND expressions
            List<Filter> nonAndList = new ArrayList<Filter>();
            // a list of AND expressions that we need to distribute
            List<Filter> andList = new ArrayList<Filter>();
            for (Filter child : ((OrFilter) current).getFilters()) {
                if (child instanceof AndFilter) {
                    andList.add(child);
                } else if (child instanceof OrFilter) {
                    // pull apart the kids of the OR expression
                    for (Filter grandChild : ((OrFilter) child).getFilters()) {
                        nonAndList.add(grandChild);
                    }
                } else {
                    nonAndList.add(child);
                }
            }
            if (!andList.isEmpty()) {
                List<Filter> result = Lists.newArrayList();
                generateAllCombinations(result, andList, nonAndList);
                return new AndFilter(result);
            }
        }
        return current;
    }

    // CNF conversion functions were adapted from Apache Hive, see:
    // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
    private static Filter flatten(Filter root) {
        if (root instanceof BooleanFilter) {
            List<Filter> children = new ArrayList<>();
            children.addAll(((BooleanFilter) root).getFilters());
            // iterate through the index, so that if we add more children,
            // they don't get re-visited
            for (int i = 0; i < children.size(); ++i) {
                Filter child = flatten(children.get(i));
                // do we need to flatten?
                if (child.getClass() == root.getClass() && !(child instanceof NotFilter)) {
                    boolean first = true;
                    List<Filter> grandKids = ((BooleanFilter) child).getFilters();
                    for (Filter grandkid : grandKids) {
                        // for the first grandkid replace the original parent
                        if (first) {
                            first = false;
                            children.set(i, grandkid);
                        } else {
                            children.add(++i, grandkid);
                        }
                    }
                } else {
                    children.set(i, child);
                }
            }
            // if we have a singleton AND or OR, just return the child
            if (children.size() == 1 && (root instanceof AndFilter || root instanceof OrFilter)) {
                return children.get(0);
            }

            if (root instanceof AndFilter) {
                return new AndFilter(children);
            } else if (root instanceof OrFilter) {
                return new OrFilter(children);
            }
        }
        return root;
    }

    // CNF conversion functions were adapted from Apache Hive, see:
    // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
    private static void generateAllCombinations(
            List<Filter> result,
            List<Filter> andList,
            List<Filter> nonAndList
    ) {
        List<Filter> children = ((AndFilter) andList.get(0)).getFilters();
        if (result.isEmpty()) {
            for (Filter child : children) {
                List<Filter> a = Lists.newArrayList(nonAndList);
                a.add(child);
                result.add(new OrFilter(a));
            }
        } else {
            List<Filter> work = new ArrayList<>(result);
            result.clear();
            for (Filter child : children) {
                for (Filter or : work) {
                    List<Filter> a = Lists.newArrayList((((OrFilter) or).getFilters()));
                    a.add(child);
                    result.add(new OrFilter(a));
                }
            }
        }
        if (andList.size() > 1) {
            generateAllCombinations(
                    result, andList.subList(1, andList.size()),
                    nonAndList
            );
        }
    }
}
