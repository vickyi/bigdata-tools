package com.aiplus.bi.etl.input.rdb;

import com.aiplus.bi.etl.input.DataInputJobConfiguration;
import com.aiplus.bi.etl.input.OutputHiveSchemaTransfer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * RDB数据输出到Hive的转换器
 *
 * @author dev
 */
public class RdbOutputHiveSchemaTransfer extends OutputHiveSchemaTransfer {

    public RdbOutputHiveSchemaTransfer(DataInputJobConfiguration etlJobConfiguration, String etlJobId) {
        super(etlJobConfiguration, etlJobId);
    }

    @Override
    protected List<String> doHiveSchemaOutput() throws IOException {
        String schemaName = getEtlJobConfiguration().getOutput().getHiveSchema();
        if ("base".equals(schemaName)) {
            schemaName = "ods";
        }
        List<String> hiveCreateStatements = new ArrayList<>();
        // 读取所有表的表结构
        try (Connection connection = getSourceConnection()) {
            DatabaseMetaData dmd = connection.getMetaData();
            for (DataInputJobConfiguration.TableMapper tableMapper : getEtlJobConfiguration().getMappers()) {
                String sourceTable = tableMapper.getSourceTable();
                String[] fields = null;
                if (null == sourceTable || "".equals(sourceTable)) {
                    sourceTable = tableMapper.getSource().getTableName();
                    fields = tableMapper.getSource().getFields();
                }
                // 通配符的情况
                if (sourceTable.contains("%")) {
                    // 拿表结构
                    try (ResultSet rs = dmd.getTables(null, "%", sourceTable, new String[]{"TABLE"})) {
                        if (rs.next()) {
                            sourceTable = rs.getString("TABLE_NAME");
                        }
                    }
                }
                List<String> colNames = new ArrayList<>();
                if (null != fields) {
                    colNames = Arrays.asList(fields);
                }
                boolean checkField = !colNames.isEmpty();
                List<ColumnMetadata> cols = new ArrayList<>(checkField ? 10 : colNames.size());
                // 拿列的信息
                try (ResultSet rs = dmd.getColumns(null, "%", sourceTable, "%")) {
                    while (rs.next()) {
                        String colName = rs.getString("COLUMN_NAME");
                        if (checkField) {
                            if (!colNames.contains(colName)) {
                                // 如果该字段不存在于需要的字段里面，则抛弃该字段
                                continue;
                            }
                        }
                        cols.add(new ColumnMetadata(colName, transferHiveDataType(rs.getInt("DATA_TYPE")), rs.getInt("COLUMN_SIZE"), rs.getInt("DECIMAL_DIGITS")));
                    }
                }
                hiveCreateStatements.add(generateCreateStatementDDL(schemaName, tableMapper.getTargetTable(), cols, getEtlJobConfiguration().getOutput().getHiveExternalPath()));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return hiveCreateStatements;
    }
}
