package com.aiplus.bi.etl.input.rdb;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据库表操作相关帮助类.
 *
 * @author dev
 */
public class TableHelper {

    private static final String DEFAULT_SPLIT_KEY = "id";

    public static void getSourceTableColumns(DatabaseMetaData dmd, Table table) throws SQLException {
        String tableName = table.getName();
        // 拿到主键
        List<String> pks = new ArrayList<>();
        try (ResultSet rs = dmd.getPrimaryKeys(null, "%", tableName)) {
            while (rs.next()) {
                pks.add(rs.getString("COLUMN_NAME"));
            }
        }
        ColumnList columnList = table.getColumnList();
        // 拿到其余列
        try (ResultSet rs = dmd.getColumns(null, "%", tableName, "%")) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                boolean isPrimaryKey = pks.contains(columnName);
                columnList.addColumn(columnName, rs.getInt("DATA_TYPE"), rs.getInt("ORDINAL_POSITION"), isPrimaryKey);
            }
        }
        // 处理完了表的所有列以后对列进行排序，按照列的索引进行倒序
        table.sortColumns(Table.COLUMN_ASC);
        table.prepared();
    }

    public static void getTableSplit(Connection connection, Table table) throws SQLException {
        // Query max,min ID
        String splitKey = null;
        if (null != table.getSplitKey() && !"".equalsIgnoreCase(table.getSplitKey())) {
            splitKey = table.getSplitKey();
        } else {
            // 如果主键只有一个并且是ID的话，则去取最大最小值
            if (table.getColumnList().getKeys().size() == 1) {
                String pk = table.getColumnList().getKeys().get(0).getName();
                if (DEFAULT_SPLIT_KEY.equalsIgnoreCase(pk)) {
                    splitKey = pk;
                    table.setSplitKey(pk);
                }
            }
        }
        if (null != splitKey) {
            // 组装SQL
            String sql = "SELECT MIN(" + splitKey + ") AS min_id,MAX(" + splitKey + ") AS max_id FROM `" + table.getName() + "`";
            try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    long start = rs.getLong("min_id");
                    // 最后一个需要+1，因为是开区间：[start, end)
                    long end = rs.getLong("max_id") + 1;
                    table.setLengthInterval(start, end);
                }
            }
        } else {
            // split key为空，则需要根据总记录数来分页
            String sql = "SELECT COUNT(0) AS total_count FROM `" + table.getName() + "`";
            try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    long length = rs.getLong("total_count");
                    table.setLengthInterval(0L, length);
                }
            }
        }
    }
}
