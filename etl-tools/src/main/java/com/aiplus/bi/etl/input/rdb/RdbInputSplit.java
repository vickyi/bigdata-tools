package com.aiplus.bi.etl.input.rdb;

import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * RDB数据的分片.
 *
 * @author dev
 */
public class RdbInputSplit extends InputSplit implements Writable {

    private static final Log LOG = LogFactory.getLog(RdbInputSplit.class);

    private String table;

    private String targetTable;

    private String splitKey;

    private String[] primaryKeys;

    private String[] fields;

    private long start;

    private long end;

    private RdbInputSplit nextSplit;
    private transient GsonBuilder gsonBuilder = new GsonBuilder().serializeNulls();

    public RdbInputSplit() {
        super();
        // 如果没有这个构造方法，会直接报错
    }

    public RdbInputSplit(String table, String targetTable, String[] primaryKeys, String splitKey, String[] fields, long start, long end) {
        this(table, targetTable, primaryKeys, splitKey, fields, start, end, null);
    }

    public RdbInputSplit(String table, String targetTable, String[] primaryKeys, String splitKey, String[] fields, long start, long end, RdbInputSplit nextSplit) {
        this.table = table;
        this.targetTable = targetTable;
        this.primaryKeys = primaryKeys;
        this.splitKey = splitKey;
        this.fields = fields;
        this.start = start;
        this.end = end;
        this.nextSplit = nextSplit;
    }

    public static RdbInputSplit createFromTable(Table table) {
        return new RdbInputSplit(table.getName(), table.getTarget(), table.getKeyNames(), table.getSplitKey(), table.getColumnNames(), table.getStart(), table.getEnd());
    }

    public static RdbInputSplit createFromTableSplit(Table table, long start, long end) {
        return new RdbInputSplit(table.getName(), table.getTarget(), table.getKeyNames(), table.getSplitKey(), table.getColumnNames(), start, end);
    }

    @Override
    public long getLength() {
        return end - start;
    }

    @Override
    public String[] getLocations() {
        return new String[]{};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        String json = gsonBuilder.create().toJson(this);
        LOG.debug("Write input split data: " + json);
        Text.writeString(dataOutput, json);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String json = Text.readString(dataInput);
        LOG.debug("Read input split data: " + json);
        RdbInputSplit that = gsonBuilder.create().fromJson(json, RdbInputSplit.class);
        this.table = that.table;
        this.targetTable = that.targetTable;
        this.splitKey = that.splitKey;
        this.primaryKeys = that.primaryKeys;
        this.fields = that.fields;
        this.start = that.start;
        this.end = that.end;
        this.nextSplit = that.nextSplit;
    }

    public boolean hasNextSplit() {
        return this.nextSplit != null;
    }

    public String getTable() {
        return table;
    }

    public String[] getFields() {
        return fields;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public RdbInputSplit getNextSplit() {
        return nextSplit;
    }

    public void setNextSplit(RdbInputSplit nextSplit) {
        this.nextSplit = nextSplit;
    }

    public String getSplitKey() {
        return splitKey;
    }

    public String[] getPrimaryKeys() {
        return primaryKeys;
    }

    @Override
    public String toString() {
        return gsonBuilder.create().toJson(this);
    }

    public String getTargetTable() {
        return targetTable;
    }
}
