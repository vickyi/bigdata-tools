package com.aiplus.bi.etl.input.druid;

import com.aiplus.bi.etl.input.DataInputJobConfiguration;
import com.aiplus.bi.etl.input.DataInputs;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SegmentInputFormat extends InputFormat<LongWritable, EventWritable> implements Configurable, DruidInputConfigurable {

    private static final Log LOG = LogFactory.getLog(SegmentInputFormat.class);

    private static final String QUERY_SEGMENT_LIST = "SELECT payload " +
            "FROM druid_segments " +
            "WHERE `used` = 1 AND datasource = ? AND convert_tz(`start`, '+00:00', '+08:00') LIKE ? " +
            "ORDER BY UNIX_TIMESTAMP(`start`) ASC, id ASC";

    private DataInputs dataInputs;

    private DataInputJobConfiguration jobConfiguration;

    public DataInputs getDataInputs() {
        return dataInputs;
    }

    public DataInputJobConfiguration getJobConfiguration() {
        return jobConfiguration;
    }

    @Override
    public Configuration getConf() {
        return this.dataInputs.getMapReduceConfiguration();
    }

    @Override
    public void setConf(Configuration conf) {
        this.dataInputs = new DataInputs(conf);
        this.jobConfiguration = dataInputs.getJobConfiguration();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = getConf();

        Map<String, SegmentInputSplit> splitMap = new LinkedHashMap<>(15);

        for (DataInputJobConfiguration.TableMapper tableMapper : dataInputs.getJobConfiguration().getMappers()) {
            splitMap.putAll(generateSplitMap(tableMapper.getSourceTable(), conf.get(DRUID_JOB_ARGS_EXPORT_DATE)));
        }
        return new ArrayList<>(splitMap.values());
    }

    private Map<String, SegmentInputSplit> generateSplitMap(String dataSource, String exportDate) throws IOException {
        Map<String, SegmentInputSplit> splitMap = new LinkedHashMap<>(15);

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        GsonBuilder gsonBuilder = new GsonBuilder().serializeNulls();
        try {
            conn = dataInputs.getSourceConnection();
            pstmt = conn.prepareStatement(QUERY_SEGMENT_LIST);
            pstmt.setString(1, dataSource);
            pstmt.setString(2, exportDate + "%");
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String strPayLoad = rs.getString("payload");
                JsonObject json = gsonBuilder.create().fromJson(strPayLoad, JsonObject.class);
                String interval = json.get("interval").getAsString();
                String dimensions = json.get("dimensions").getAsString();
                String metrics = json.get("metrics").getAsString();
                String key = dataSource + "^" + interval;
                SegmentInputSplit sis = splitMap.get(key);
                if (null == sis) {
                    sis = new SegmentInputSplit(dataSource, interval, dimensions, metrics);
                    splitMap.put(key, sis);
                }
                String dataId = json.get("identifier").getAsString();
                long dataSize = json.get("size").getAsLong();
                JsonObject dataShardJson = json.getAsJsonObject("loadSpec");
                String dataType = dataShardJson.get("type").getAsString();
                String dataPath = dataShardJson.get("path").getAsString();
                sis.addSegmentData(createSegmentData(dataId, dataType, dataPath, dataSize));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            if (null != rs) {
                try {
                    rs.close();
                } catch (SQLException ignored) {
                }
            }
            if (null != pstmt) {
                try {
                    pstmt.close();
                } catch (SQLException ignored) {
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (SQLException ignored) {
                }
            }
        }
        return splitMap;
    }

    private SegmentInputSplit.SegmentData createSegmentData(String dataId, String dataType, String dataPath, long dataSize) {
        SegmentInputSplit.SegmentData segmentData = new SegmentInputSplit.SegmentData();
        segmentData.setDataId(dataId);
        segmentData.setDataType(dataType);
        segmentData.setDataPath(dataPath);
        segmentData.setDataSize(dataSize);
        return segmentData;
    }

    @Override
    public RecordReader<LongWritable, EventWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new SegmentRecordReader(getConf(), (SegmentInputSplit) split);
    }
}
