package com.aiplus.bi.etl.input.druid;

import com.aiplus.bi.etl.JobTools;
import com.fasterxml.jackson.databind.InjectableValues;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.Yielders;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.Druids;
import io.druid.query.Result;
import io.druid.query.select.*;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author dev
 */
public class SegmentRecordReader extends RecordReader<LongWritable, EventWritable> implements DruidInputConfigurable {

    private static final Log LOG = LogFactory.getLog(SegmentRecordReader.class);

    private Configuration conf;

    private SegmentInputSplit split;

    private LongWritable key = null;

    private EventWritable value = null;

    private long pos = 0L;

    private long maxRecordNum = 0L;

    private String localDataDirPath;

    private QueryableIndex[] segmentIndexArray;

    private EventHolder currentEventHolder;

    private int currentQueryableIndexI = -1;

    private Iterator<EventHolder> currentEventHolderIterator = null;

    public SegmentRecordReader(Configuration conf, SegmentInputSplit split) {
        this.split = split;
        this.conf = conf;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        LOG.info("initialize druid segment index data....");
        // downloading segment data from remote file system.
        String strDruidDataLocalDir = conf.get(DRUID_DATA_LOCAL_DIR, "/tmp");
        final InjectableValues.Std injectableValues = new InjectableValues.Std();
        injectableValues.addValue(ExprMacroTable.class, ExprMacroTable.nil());

        IndexIO indexIO = new IndexIO(
                new DefaultObjectMapper().setInjectableValues(injectableValues),
                OffHeapMemorySegmentWriteOutMediumFactory.instance(),
                () -> 0
        );
        // 这里支持Druid的Deep Storage是存储在HDFS上的情况，使用hadoop fs -get命令可以兼容hdfs和oss的文件协议
        SegmentInputSplit sis = (SegmentInputSplit) split;
        localDataDirPath = strDruidDataLocalDir + "/" + sis.getSegmentId();
        FileUtils.forceMkdir(new File(localDataDirPath));
        SegmentInputSplit.SegmentData[] segmentDataArray = sis.getSegments();
        segmentIndexArray = new QueryableIndex[segmentDataArray.length];
        String localIndexDataZipFileName;
        for (int i = 0; i < segmentDataArray.length; i++) {
            String segmentRemoteLocation = segmentDataArray[i].getDataPath();
            LOG.info("Downloading druid[" + sis.getDataSource() + "] segment: " + segmentRemoteLocation);
            if (segmentRemoteLocation.startsWith("oss://")) {
                JobTools.runCommand(context.getJobID().toString(), new String[]{
                        "ossutil",
                        "cp",
                        segmentRemoteLocation,
                        localDataDirPath
                });
                localIndexDataZipFileName = localDataDirPath + getOSSRemoteLocalLocationDir(segmentRemoteLocation);
            } else {
                JobTools.runCommand(context.getJobID().toString(), new String[]{
                        "hadoop",
                        "fs",
                        "-get",
                        segmentRemoteLocation,
                        localDataDirPath
                });
                localIndexDataZipFileName = localDataDirPath + "/" + FilenameUtils.getName(segmentRemoteLocation);
            }
            // unzip
            LOG.info("Load queryable segment index: " + segmentDataArray[i].getDataId());
            File indexDataDir = new File(localDataDirPath, segmentDataArray[i].getDataId());
            JobTools.unZip(new File(localIndexDataZipFileName), indexDataDir);
            segmentIndexArray[i] = indexIO.loadIndex(indexDataDir);
            // 计算总行数
            maxRecordNum = maxRecordNum + segmentIndexArray[i].getNumRows();
        }
    }

    private String getOSSRemoteLocalLocationDir(String segmentRemoteLocation) {
        String s = segmentRemoteLocation.substring("oss://".length());

        return s.substring(s.indexOf("/"));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (null == key) {
            key = new LongWritable();
        }
        if (null == value) {
            value = new EventWritable();
        }
        if (!preparedNext()) {
            return false;
        }
        key.set(pos);
        value.readFields(split.getDataSource(), split.getInterval(), currentEventHolder.getTimestamp(), currentEventHolder.getEvent());
        pos++;
        return true;
    }

    private boolean preparedNext() {
        if (null == segmentIndexArray || 0 == segmentIndexArray.length) {
            return false;
        }
        if (pos == (maxRecordNum - 1)) {
            return false;
        }
        if (pos == 0L) {
            this.currentQueryableIndexI = 0;
        }
        if (null == this.currentEventHolderIterator) {
            generateCurrentEventHolderIterator();
        }
        if (this.currentEventHolderIterator.hasNext()) {
            this.currentEventHolder = this.currentEventHolderIterator.next();
        } else {
            // 如果没有后面的了，那么需要判断是否继续往后面加载
            if (currentQueryableIndexI == (segmentIndexArray.length - 1)) {
                return false;
            }
            this.currentQueryableIndexI++;
            generateCurrentEventHolderIterator();
        }
        return true;
    }

    private void generateCurrentEventHolderIterator() {
        QueryableIndex indexData = segmentIndexArray[currentQueryableIndexI];
        SelectQueryEngine selectQueryEngine = new SelectQueryEngine();
        Druids.SelectQueryBuilder selectQueryBuilder = Druids.newSelectQueryBuilder();
        SelectQuery selectQuery = selectQueryBuilder.dataSource(split.getDataSource())
                .intervals(split.getInterval())
                .dimensions(Arrays.asList(split.getDimensions().split(",")))
                .metrics(Arrays.asList(split.getMetrics().split(",")))
                .pagingSpec(PagingSpec.newSpec(indexData.getNumRows()))
                .build();
        Sequence<Result<SelectResultValue>> resultSequence = selectQueryEngine.process(selectQuery, new QueryableIndexSegment(split.getSegmentId(), indexData));
        Yielder<Result<SelectResultValue>> yielder = Yielders.each(resultSequence);
        this.currentEventHolderIterator = yielder.get().getValue().getEvents().iterator();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public EventWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return maxRecordNum == 0L ? 1.0F : pos / (float) maxRecordNum;
    }

    @Override
    public void close() throws IOException {
        // close queryable index
        for (QueryableIndex queryableIndex : segmentIndexArray) {
            queryableIndex.close();
        }
        // clean dir
        FileUtils.forceDelete(new File(localDataDirPath));
    }
}
