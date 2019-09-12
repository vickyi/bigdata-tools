package com.aiplus.bi.etl.input.druid;

import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dev
 */
public class SegmentInputSplit extends InputSplit implements Writable {

    private static final Log LOG = LogFactory.getLog(SegmentInputSplit.class);

    private transient GsonBuilder gsonBuilder = new GsonBuilder().serializeNulls();

    private String segmentId;

    private String dataSource;

    private String interval;

    private String dimensions;

    private String metrics;

    private SegmentData[] segments;

    private long length = 0L;

    private String[] locations;

    private transient List<SegmentData> segmentDataList = new ArrayList<>();

    public SegmentInputSplit() {
        super();
    }

    public SegmentInputSplit(String dataSource, String interval, String dimensions, String metrics) {
        this.dataSource = dataSource;
        this.interval = interval;
        this.dimensions = dimensions;
        this.metrics = metrics;
        this.segmentId = dataSource + "_" + interval.replaceAll(":", "-").replaceAll("\\.", "-").replaceAll("/", "_");
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{};
    }

    public void setLocations(String[] locations) {
        this.locations = locations;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 转换成数组，这样在序列化的时候所占用的资源就会少很多，也会提升序列化的速度.
        this.segments = segmentDataList.toArray(new SegmentData[0]);
        this.locations = new String[segments.length];
        for (int i = 0; i < segments.length; i++) {
            this.locations[i] = segments[i].getDataPath();
        }
        String json = gsonBuilder.create().toJson(this);
        LOG.debug("Write input split data: " + json);
        Text.writeString(dataOutput, json);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String json = Text.readString(dataInput);
        LOG.debug("Read input split data: " + json);
        SegmentInputSplit that = gsonBuilder.create().fromJson(json, SegmentInputSplit.class);
        this.dataSource = that.dataSource;
        this.interval = that.interval;
        this.dimensions = that.dimensions;
        this.metrics = that.metrics;
        this.segments = that.segments;
        this.length = that.length;
        this.locations = that.locations;
        this.segmentId = that.segmentId;
    }

    public void addSegmentData(SegmentData segmentData) {
        segmentDataList.add(segmentData);
    }

    public String getDimensions() {
        return dimensions;
    }

    public void setDimensions(String dimensions) {
        this.dimensions = dimensions;
    }

    public String getMetrics() {
        return metrics;
    }

    public void setMetrics(String metrics) {
        this.metrics = metrics;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getInterval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }

    public SegmentData[] getSegments() {
        return segments;
    }

    public void setSegments(SegmentData[] segments) {
        this.segments = segments;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(String segmentId) {
        this.segmentId = segmentId;
    }

    public static class SegmentData implements Serializable {
        private String dataType;

        private String dataPath;

        private String dataId;

        private long dataSize;

        public String getDataType() {
            return dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }

        public String getDataPath() {
            return dataPath;
        }

        public void setDataPath(String dataPath) {
            this.dataPath = dataPath;
        }

        public String getDataId() {
            return dataId;
        }

        public void setDataId(String dataId) {
            this.dataId = dataId;
        }

        public long getDataSize() {
            return dataSize;
        }

        public void setDataSize(long dataSize) {
            this.dataSize = dataSize;
        }
    }
}
