package com.aiplus.bi.etl.input.rdb;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 行记录读取器.根据数据分片信息一行一行读取数据输出给Map.
 *
 * @author dev
 */
public class RowRecordReader extends RecordReader<LongWritable, RowWritable> {

    private static final Log LOG = LogFactory.getLog(RowRecordReader.class);

    private Configuration conf;

    private RdbInputSplit split;

    private ResultSet rs;

    private LongWritable key = null;

    private RowWritable value = null;

    private Connection connection;

    private PreparedStatement pstmt;

    private long pos;

    private long maxRecordNum;

    public RowRecordReader(Configuration conf, RdbInputSplit split, Connection connection) {
        this.conf = conf;
        this.split = split;
        this.connection = connection;
        RdbInputSplit eis = split;
        while (eis.hasNextSplit()) {
            this.maxRecordNum = this.maxRecordNum + eis.getLength();
            eis = eis.getNextSplit();
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
        // do nothing.
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        try {
            if (key == null) {
                key = new LongWritable();
            }
            if (value == null) {
                value = new RowWritable();
            }
            if (null == this.rs) {
                // First time into this method, run the query.
                this.rs = executeQuery(getSelectQuery());
            }
            if (!checkNext()) {
                return false;
            }
            // Set the key field value as the output key value
            key.set(pos + split.getStart());

            value.readFields(rs, split.getTable(), split.getFields(), split.getPrimaryKeys(), split.getTargetTable());

            pos++;
        } catch (SQLException e) {
            throw new IOException("SQLException in nextKeyValue", e);
        }
        return true;
    }

    private boolean checkNext() throws IOException, SQLException {
        // 判断当前结果集有没有查询完
        if (!rs.next()) {
            // 如果没有查询完
            // 判断有没有下一个分片需要查询
            if (split.hasNextSplit()) {
                // 轮换到下一个分片进行处理
                this.split = this.split.getNextSplit();
                closeResultSet(rs);
                // 资源返还
                this.rs = null;
                // 重新查询
                this.rs = executeQuery(getSelectQuery());
                // 递归处理查询的情况，直到找到结果集或者没有需要处理的分片了
                return checkNext();
            } else {
                // 当前结果集查询完了，并且当前分片之后没有分片需要处理了，直接返回False，表示没有剩下的数据了
                return false;
            }
        }
        // 表示当前结果集中还有数据，继续往下处理
        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public RowWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return this.pos / (float) this.maxRecordNum;
    }

    @Override
    public void close() throws IOException {
        try {
            if (null != rs) {
                rs.close();
            }
            if (null != pstmt) {
                pstmt.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    protected ResultSet executeQuery(String query) throws SQLException {
        LOG.info("Query SQL: " + query);
        this.pstmt = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        return pstmt.executeQuery();
    }

    @VisibleForTesting
    protected String getSelectQuery(RdbInputSplit split) {
        StringBuilder query = new StringBuilder();

        query.append("SELECT ");

        String[] primaryKeys = split.getPrimaryKeys();
        String[] fieldNames = split.getFields();
        String tableName = split.getTable();

        query.append("CONCAT(");
        StringBuilder pkQuery = new StringBuilder();
        for (int i = 0; i < primaryKeys.length - 1; i++) {
            String primaryKey = primaryKeys[i];
            query.append("`").append(primaryKey).append("`, '_',");
            pkQuery.append("`").append(primaryKey).append("`,");
        }
        String lastPrimaryKey = primaryKeys[primaryKeys.length - 1];
        query.append("`").append(lastPrimaryKey).append("`) AS table_pk, ");
        pkQuery.append("`").append(lastPrimaryKey).append("`");

        query.append(pkQuery.toString()).append(", ");

        for (int i = 0; i < fieldNames.length; i++) {
            query.append("`").append(fieldNames[i]).append("`");
            if (i != fieldNames.length - 1) {
                query.append(", ");
            }
        }

        query.append(" FROM `").append(tableName).append("`");

        String splitKey = split.getSplitKey();
        if (splitKey == null) {
            query.append(" LIMIT ").append(split.getLength());
            query.append(" OFFSET ").append(split.getStart());
        } else {
            query.append(" WHERE ");
            query.append("`").append(splitKey).append("`").append(" >= ").append(split.getStart());
            query.append(" AND ");
            query.append("`").append(splitKey).append("`").append(" < ").append(split.getEnd());
        }

        return query.toString();
    }

    private String getSelectQuery() {
        return getSelectQuery(split);
    }

    private void closeResultSet(ResultSet rs) throws IOException {
        try {
            if (null != rs && rs.isClosed()) {
                rs.close();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public Configuration getConf() {
        return conf;
    }
}
