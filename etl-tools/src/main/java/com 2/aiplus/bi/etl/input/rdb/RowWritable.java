package com.aiplus.bi.etl.input.rdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author dev
 */
public class RowWritable {

    private static final String COLUMN_SPLIT = "\001";

    private static final String HIVE_NULL_VALUE = "\\N";

    private static final String TABLE_NULL_VALUE = "null";

    private static final Pattern LINE_RN_PATTERN = Pattern.compile("[\r\n]");

    private String tableName;

    private String dataText;

    private String targetTable;

    public void readFields(ResultSet rs, String tableName, String[] fieldNames, String[] primaryKeyNames, String targetTable) throws SQLException {
        // 把所有的数据都读取为一个字符串就行了，不需要特殊处理
        this.tableName = tableName;
        this.targetTable = targetTable;
        //
        StringBuilder val = new StringBuilder();
        // table pk
        val.append(rs.getString("table_pk")).append(COLUMN_SPLIT);
        // 主键
        for (String primaryKeyName : primaryKeyNames) {
            val.append(rs.getString(primaryKeyName)).append(COLUMN_SPLIT);
        }
        // 字段
        for (int i = 0; i < fieldNames.length - 1; i++) {
            val.append(checkText(rs.getString(fieldNames[i]))).append(COLUMN_SPLIT);
        }
        val.append(checkText(rs.getString(fieldNames[fieldNames.length - 1])));
        val.trimToSize();

        this.dataText = val.toString();
    }

    private Object checkText(String obj) {
        if (null == obj || TABLE_NULL_VALUE.equalsIgnoreCase(obj)) {
            return HIVE_NULL_VALUE;
        }
        // 去掉换行符
        Matcher m = LINE_RN_PATTERN.matcher(obj);
        return m.replaceAll("");
    }

    public String getDataText() {
        return this.dataText;
    }


    public String getTableName() {
        return tableName;
    }

    public String getTargetTable() {
        return targetTable;
    }
}
