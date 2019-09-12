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

package io.druid.segment;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.druid.collections.bitmap.ConciseBitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.spatial.ImmutableRTree;
import io.druid.common.utils.SerializerUtils;
import io.druid.java.util.common.*;
import io.druid.java.util.common.io.smoosh.Smoosh;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.segment.column.*;
import io.druid.segment.data.*;
import io.druid.segment.serde.*;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Interval;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Set;

public class IndexIO {
    public static final byte V8_VERSION = 0x8;
    public static final byte V9_VERSION = 0x9;
    public static final int CURRENT_VERSION_ID = V9_VERSION;

    public static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();
    private static final EmittingLogger log = new EmittingLogger(IndexIO.class);
    private static final SerializerUtils serializerUtils = new SerializerUtils();
    private final Map<Integer, IndexLoader> indexLoaders;
    private final ObjectMapper mapper;
    private final SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory;

    @Inject
    public IndexIO(ObjectMapper mapper, SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory, ColumnConfig columnConfig) {
        this.mapper = Preconditions.checkNotNull(mapper, "null ObjectMapper");
        this.defaultSegmentWriteOutMediumFactory = Preconditions.checkNotNull(defaultSegmentWriteOutMediumFactory, "null SegmentWriteOutMediumFactory");
        Preconditions.checkNotNull(columnConfig, "null ColumnConfig");
        ImmutableMap.Builder<Integer, IndexLoader> indexLoadersBuilder = ImmutableMap.builder();
        LegacyIndexLoader legacyIndexLoader = new LegacyIndexLoader(new DefaultIndexIOHandler(), columnConfig);
        for (int i = 0; i <= V8_VERSION; i++) {
            indexLoadersBuilder.put(i, legacyIndexLoader);
        }
        indexLoadersBuilder.put((int) V9_VERSION, new V9IndexLoader(columnConfig));
        indexLoaders = indexLoadersBuilder.build();
    }

    public static int getVersionFromDir(File inDir) throws IOException {
        File versionFile = new File(inDir, "version.bin");
        if (versionFile.exists()) {
            return Ints.fromByteArray(Files.toByteArray(versionFile));
        }

        final File indexFile = new File(inDir, "index.drd");
        int version;
        try (InputStream in = new FileInputStream(indexFile)) {
            version = in.read();
        }
        return version;
    }

    public static void checkFileSize(File indexFile) throws IOException {
        final long fileSize = indexFile.length();
        if (fileSize > Integer.MAX_VALUE) {
            throw new IOE("File[%s] too large[%d]", indexFile, fileSize);
        }
    }

    public static File makeDimFile(File dir, String dimension) {
        return new File(dir, StringUtils.format("dim_%s.drd", dimension));
    }

    public static File makeTimeFile(File dir, ByteOrder order) {
        return new File(dir, StringUtils.format("time_%s.drd", order));
    }

    public static File makeMetricFile(File dir, String metricName, ByteOrder order) {
        return new File(dir, StringUtils.format("met_%s_%s.drd", metricName, order));
    }

    public QueryableIndex loadIndex(File inDir) throws IOException {
        final int version = SegmentUtils.getVersionFromDir(inDir);

        final IndexLoader loader = indexLoaders.get(version);

        if (loader != null) {
            return loader.load(inDir, mapper);
        } else {
            throw new ISE("Unknown index version[%s]", version);
        }
    }

    interface IndexIOHandler {
        MMappedIndex mapDir(File inDir) throws IOException;
    }

    interface IndexLoader {
        QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException;
    }

    public static class DefaultIndexIOHandler implements IndexIOHandler {
        private static final Logger log = new Logger(DefaultIndexIOHandler.class);

        @Override
        public MMappedIndex mapDir(File inDir) throws IOException {
            log.debug("Mapping v8 index[%s]", inDir);
            long startTime = System.currentTimeMillis();

            InputStream indexIn = null;
            try {
                indexIn = new FileInputStream(new File(inDir, "index.drd"));
                byte theVersion = (byte) indexIn.read();
                if (theVersion != V8_VERSION) {
                    throw new IAE("Unknown version[%d]", theVersion);
                }
            } finally {
                Closeables.close(indexIn, false);
            }

            SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);
            ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");

            indexBuffer.get(); // Skip the version byte
            final GenericIndexed<String> availableDimensions = GenericIndexed.read(
                    indexBuffer,
                    GenericIndexed.STRING_STRATEGY,
                    smooshedFiles
            );
            final GenericIndexed<String> availableMetrics = GenericIndexed.read(
                    indexBuffer,
                    GenericIndexed.STRING_STRATEGY,
                    smooshedFiles
            );
            final Interval dataInterval = Intervals.of(serializerUtils.readString(indexBuffer));
            final BitmapSerdeFactory bitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();

            CompressedColumnarLongsSupplier timestamps = CompressedColumnarLongsSupplier.fromByteBuffer(
                    smooshedFiles.mapFile(makeTimeFile(inDir, BYTE_ORDER).getName()),
                    BYTE_ORDER
            );

            Map<String, MetricHolder> metrics = Maps.newLinkedHashMap();
            for (String metric : availableMetrics) {
                final String metricFilename = makeMetricFile(inDir, metric, BYTE_ORDER).getName();
                final MetricHolder holder = MetricHolder.fromByteBuffer(smooshedFiles.mapFile(metricFilename), smooshedFiles);

                if (!metric.equals(holder.getName())) {
                    throw new ISE("Metric[%s] loaded up metric[%s] from disk.  File names do matter.", metric, holder.getName());
                }
                metrics.put(metric, holder);
            }

            Map<String, GenericIndexed<String>> dimValueLookups = Maps.newHashMap();
            Map<String, VSizeColumnarMultiInts> dimColumns = Maps.newHashMap();
            Map<String, GenericIndexed<ImmutableBitmap>> bitmaps = Maps.newHashMap();

            for (String dimension : IndexedIterable.create(availableDimensions)) {
                ByteBuffer dimBuffer = smooshedFiles.mapFile(makeDimFile(inDir, dimension).getName());
                String fileDimensionName = serializerUtils.readString(dimBuffer);
                Preconditions.checkState(
                        dimension.equals(fileDimensionName),
                        "Dimension file[%s] has dimension[%s] in it!?",
                        makeDimFile(inDir, dimension),
                        fileDimensionName
                );

                dimValueLookups.put(dimension, GenericIndexed.read(dimBuffer, GenericIndexed.STRING_STRATEGY));
                dimColumns.put(dimension, VSizeColumnarMultiInts.readFromByteBuffer(dimBuffer));
            }

            ByteBuffer invertedBuffer = smooshedFiles.mapFile("inverted.drd");
            for (int i = 0; i < availableDimensions.size(); ++i) {
                bitmaps.put(
                        serializerUtils.readString(invertedBuffer),
                        GenericIndexed.read(invertedBuffer, bitmapSerdeFactory.getObjectStrategy())
                );
            }

            Map<String, ImmutableRTree> spatialIndexed = Maps.newHashMap();
            ByteBuffer spatialBuffer = smooshedFiles.mapFile("spatial.drd");
            while (spatialBuffer != null && spatialBuffer.hasRemaining()) {
                spatialIndexed.put(
                        serializerUtils.readString(spatialBuffer),
                        new ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory()).fromByteBufferWithSize(
                                spatialBuffer
                        )
                );
            }

            final MMappedIndex retVal = new MMappedIndex(
                    availableDimensions,
                    availableMetrics,
                    dataInterval,
                    timestamps,
                    metrics,
                    dimValueLookups,
                    dimColumns,
                    bitmaps,
                    spatialIndexed,
                    smooshedFiles
            );

            log.debug("Mapped v8 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

            return retVal;
        }
    }

    static class LegacyIndexLoader implements IndexLoader {
        private final IndexIOHandler legacyHandler;
        private final ColumnConfig columnConfig;

        LegacyIndexLoader(IndexIOHandler legacyHandler, ColumnConfig columnConfig) {
            this.legacyHandler = legacyHandler;
            this.columnConfig = columnConfig;
        }

        @Override
        public QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException {
            MMappedIndex index = legacyHandler.mapDir(inDir);

            Map<String, Column> columns = Maps.newHashMap();

            for (String dimension : index.getAvailableDimensions()) {
                ColumnBuilder builder = new ColumnBuilder()
                        .setType(ValueType.STRING)
                        .setHasMultipleValues(true)
                        .setDictionaryEncodedColumn(
                                new DictionaryEncodedColumnSupplier(
                                        index.getDimValueLookup(dimension),
                                        null,
                                        Suppliers.ofInstance(index.getDimColumn(dimension)),
                                        columnConfig.columnCacheSizeBytes()
                                )
                        )
                        .setBitmapIndex(
                                new BitmapIndexColumnPartSupplier(
                                        new ConciseBitmapFactory(),
                                        index.getBitmapIndexes().get(dimension),
                                        index.getDimValueLookup(dimension)
                                )
                        );
                if (index.getSpatialIndexes().get(dimension) != null) {
                    builder.setSpatialIndex(
                            new SpatialIndexColumnPartSupplier(
                                    index.getSpatialIndexes().get(dimension)
                            )
                    );
                }
                columns.put(
                        dimension,
                        builder.build()
                );
            }

            for (String metric : index.getAvailableMetrics()) {
                final MetricHolder metricHolder = index.getMetricHolder(metric);
                if (metricHolder.getType() == MetricHolder.MetricType.FLOAT) {
                    columns.put(
                            metric,
                            new ColumnBuilder()
                                    .setType(ValueType.FLOAT)
                                    .setGenericColumn(new FloatGenericColumnSupplier(metricHolder.floatType))
                                    .build()
                    );
                } else if (metricHolder.getType() == MetricHolder.MetricType.COMPLEX) {
                    columns.put(
                            metric,
                            new ColumnBuilder()
                                    .setType(ValueType.COMPLEX)
                                    .setComplexColumn(
                                            new ComplexColumnPartSupplier(
                                                    metricHolder.getTypeName(), (GenericIndexed) metricHolder.complexType
                                            )
                                    )
                                    .build()
                    );
                }
            }

            Set<String> colSet = Sets.newTreeSet();
            for (String dimension : index.getAvailableDimensions()) {
                colSet.add(dimension);
            }
            for (String metric : index.getAvailableMetrics()) {
                colSet.add(metric);
            }

            String[] cols = colSet.toArray(new String[colSet.size()]);
            columns.put(
                    Column.TIME_COLUMN_NAME, new ColumnBuilder()
                            .setType(ValueType.LONG)
                            .setGenericColumn(new LongGenericColumnSupplier(index.timestamps))
                            .build()
            );
            return new SimpleQueryableIndex(
                    index.getDataInterval(),
                    new ArrayIndexed<>(cols, String.class),
                    index.getAvailableDimensions(),
                    new ConciseBitmapFactory(),
                    columns,
                    index.getFileMapper(),
                    null
            );
        }
    }

    static class V9IndexLoader implements IndexLoader {
        private final ColumnConfig columnConfig;

        V9IndexLoader(ColumnConfig columnConfig) {
            this.columnConfig = columnConfig;
        }

        @Override
        public QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException {
            log.debug("Mapping v9 index[%s]", inDir);
            long startTime = System.currentTimeMillis();

            final int theVersion = Ints.fromByteArray(Files.toByteArray(new File(inDir, "version.bin")));
            if (theVersion != V9_VERSION) {
                throw new IAE("Expected version[9], got[%d]", theVersion);
            }

            SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);

            ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");
            /**
             * Index.drd should consist of the segment version, the columns and dimensions of the segment as generic
             * indexes, the interval start and end millis as longs (in 16 bytes), and a bitmap index type.
             */
            final GenericIndexed<String> cols = GenericIndexed.read(
                    indexBuffer,
                    GenericIndexed.STRING_STRATEGY,
                    smooshedFiles
            );
            final GenericIndexed<String> dims = GenericIndexed.read(
                    indexBuffer,
                    GenericIndexed.STRING_STRATEGY,
                    smooshedFiles
            );
            final Interval dataInterval = Intervals.utc(indexBuffer.getLong(), indexBuffer.getLong());
            final BitmapSerdeFactory segmentBitmapSerdeFactory;

            /**
             * This is a workaround for the fact that in v8 segments, we have no information about the type of bitmap
             * index to use. Since we cannot very cleanly build v9 segments directly, we are using a workaround where
             * this information is appended to the end of index.drd.
             */
            if (indexBuffer.hasRemaining()) {
                segmentBitmapSerdeFactory = mapper.readValue(serializerUtils.readString(indexBuffer), BitmapSerdeFactory.class);
            } else {
                segmentBitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();
            }

            Metadata metadata = null;
            ByteBuffer metadataBB = smooshedFiles.mapFile("metadata.drd");
            if (metadataBB != null) {
                try {
                    metadata = mapper.readValue(
                            serializerUtils.readBytes(metadataBB, metadataBB.remaining()),
                            Metadata.class
                    );
                } catch (JsonParseException | JsonMappingException ex) {
                    // Any jackson deserialization errors are ignored e.g. if metadata contains some aggregator which
                    // is no longer supported then it is OK to not use the metadata instead of failing segment loading
                    log.warn(ex, "Failed to load metadata for segment [%s]", inDir);
                } catch (IOException ex) {
                    throw new IOException("Failed to read metadata", ex);
                }
            }

            Map<String, Column> columns = Maps.newHashMap();

            for (String columnName : cols) {
                columns.put(columnName, deserializeColumn(mapper, smooshedFiles.mapFile(columnName), smooshedFiles));
            }

            columns.put(Column.TIME_COLUMN_NAME, deserializeColumn(mapper, smooshedFiles.mapFile("__time"), smooshedFiles));

            final QueryableIndex index = new SimpleQueryableIndex(
                    dataInterval, cols, dims, segmentBitmapSerdeFactory.getBitmapFactory(), columns, smooshedFiles, metadata
            );

            log.debug("Mapped v9 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

            return index;
        }

        private Column deserializeColumn(ObjectMapper mapper, ByteBuffer byteBuffer, SmooshedFileMapper smooshedFiles)
                throws IOException {
            ColumnDescriptor serde = mapper.readValue(
                    serializerUtils.readString(byteBuffer), ColumnDescriptor.class
            );
            return serde.read(byteBuffer, columnConfig, smooshedFiles);
        }
    }
}
