package com.aiplus.bi.etl.input;

import com.aiplus.bi.etl.JobTools;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static java.sql.Types.*;

/**
 * RDB数据输出到Hive的转换器
 *
 * @author dev
 */
public abstract class OutputHiveSchemaTransfer {

    private static final Log LOG = LogFactory.getLog(OutputHiveSchemaTransfer.class);
    private DataInputJobConfiguration etlJobConfiguration;
    private String etlJobId;

    public OutputHiveSchemaTransfer(DataInputJobConfiguration etlJobConfiguration, String etlJobId) {
        this.etlJobConfiguration = etlJobConfiguration;
        this.etlJobId = etlJobId;
    }

    public String output() throws IOException {

        List<String> hiveCreateStatements = doHiveSchemaOutput();
        if (hiveCreateStatements.isEmpty()) {
            return null;
        }
        // 建表情况，无论结果如何都将建表语句写入到文件中
        String fileName = "hive_sql_" + etlJobId + ".sql";
        File hiveFile = new File(fileName);
        try (FileWriter fw = new FileWriter(hiveFile)) {
            BufferedWriter out = new BufferedWriter(fw);
            for (String hiveCreateStatement : hiveCreateStatements) {
                out.write(hiveCreateStatement);
                out.newLine();
            }
            out.flush();
            out.close();
        }
        String sqlFilePath = hiveFile.getAbsolutePath();
        if (etlJobConfiguration.getOutput().isCreateHiveTable()) {
            // 如果需要新建表，则调用Hive命令新建表
            // 调用hive -f执行命令
            JobTools.runCommand(etlJobId, new String[]{"hive", "-f", sqlFilePath});
            LOG.info("Created hive table(s) successful with [" + sqlFilePath + "].");
            // 清理导出的文件目录
            String tempDataDir = etlJobConfiguration.getOutput().getHiveExternalPath() + "/" + etlJobId;
            LOG.info("Clean temp data dir: " + tempDataDir);
            JobTools.runCommand(etlJobId, new String[]{"hadoop", "fs", "-rmr", tempDataDir});
        } else {
            // 打印出建表语句的文件路径到日志
            LOG.info("Hive DDL write into " + sqlFilePath);
        }
        return sqlFilePath;
    }

    protected abstract List<String> doHiveSchemaOutput() throws IOException;

    protected String generateCreateStatementDDL(String schemaName, String tableName, List<ColumnMetadata> cols, String externalPath) {
        boolean isExternalTable = null != externalPath && !"".equals(externalPath);
        StringBuilder ddl = new StringBuilder();
        ddl.append("DROP TABLE IF EXISTS ").append(schemaName).append(".`").append(tableName).append("`;");
        ddl.append("CREATE ");
        if (isExternalTable) {
            // 是外表则加入外表的关键字
            ddl.append("EXTERNAL ");
        }
        ddl.append("TABLE ").append(schemaName).append(".`").append(tableName).append("`(");
        // 加入table_pk
        ddl.append("`table_pk` string, ");
        for (int i = 0; i < cols.size() - 1; i++) {
            ColumnMetadata col = cols.get(i);
            ddl.append("`").append(col.getName()).append("` ").append(getDataTypeInfo(col)).append(", ");
        }
        ColumnMetadata lastCol = cols.get(cols.size() - 1);
        ddl.append("`").append(lastCol.getName()).append("` ").append(getDataTypeInfo(lastCol)).append(")");
        if (isExternalTable) {
            // 是外表则加入外表的location
            ddl.append("LOCATION \"").append(externalPath).append("/").append(etlJobId).append("/").append(tableName).append("\"");
        }
        ddl.append(";");
        // 添加数据转移的语句，先把之前的表删除掉
        String dbName = "ods";
        if (tableName.contains(".")) {
            dbName = tableName.split(".")[0];
            tableName = tableName.split(".")[1];
        }
        ddl.append("DROP TABLE IF EXISTS base.`").append(tableName).append("`;");
        ddl.append("DROP TABLE IF EXISTS ").append("`").append(dbName).append("`.`").append(tableName).append("`;");
        ddl.append("CREATE TABLE ").append("`").append(dbName).append("`.`").append(tableName).append("` AS SELECT * FROM ").append(schemaName).append(".`").append(tableName).append("`;");
        ddl.append("DROP TABLE IF EXISTS ").append(schemaName).append(".`").append(tableName).append("`;");
        ddl.trimToSize();
        return ddl.toString();
    }

    protected Connection getSourceConnection() throws SQLException {
        if (etlJobConfiguration.getSource().getUsername() == null) {
            return DriverManager.getConnection(etlJobConfiguration.getSource().getUrl());
        } else {
            return DriverManager.getConnection(
                    etlJobConfiguration.getSource().getUrl(),
                    etlJobConfiguration.getSource().getUsername(),
                    etlJobConfiguration.getSource().getPassword()
            );
        }
    }

    private String getDataTypeInfo(ColumnMetadata columnMetadata) {
        String dt = columnMetadata.getDataType().toString().toLowerCase();
        if (columnMetadata.getDataType() == DataType.DECIMAL) {
            return dt + "(" + columnMetadata.getColumnSize() + "," + columnMetadata.getDecimalDigits() + ")";
        }
        return dt;
    }

    protected DataType transferHiveDataType(int javaSqlType) {
        switch (javaSqlType) {
            case BIGINT:
                return DataType.BIGINT;
            case INTEGER:
            case TINYINT:
            case SMALLINT:
                return DataType.INT;
            case DOUBLE:
            case DECIMAL:
            case FLOAT:
                return DataType.DECIMAL;
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                return DataType.TIMESTAMP;
            case DATE:
                return DataType.DATE;
            default:
                return DataType.STRING;
        }
    }

    public DataInputJobConfiguration getEtlJobConfiguration() {
        return this.etlJobConfiguration;
    }

    public String getEtlJobId() {
        return this.etlJobId;
    }

    /**
     * Hive的数据类型
     */
    public enum DataType {
        /**
         * 字符串类型
         */
        STRING,
        /**
         * 整型
         */
        INT,

        /**
         * 长整型
         */
        BIGINT,

        /**
         * 高精度
         */
        DECIMAL,

        /**
         * 日期类型
         */
        DATE,

        /**
         * 时间戳
         */
        TIMESTAMP
    }

    public class ColumnMetadata {
        private String name;
        private DataType dataType;
        private int columnSize;
        private int decimalDigits;

        public ColumnMetadata(String name, DataType dataType, int columnSize, int decimalDigits) {
            this.name = name;
            this.dataType = dataType;
            this.columnSize = columnSize;
            this.decimalDigits = decimalDigits;
        }

        public String getName() {
            return name;
        }

        public DataType getDataType() {
            return dataType;
        }

        public int getColumnSize() {
            return columnSize;
        }

        public int getDecimalDigits() {
            return decimalDigits;
        }
    }
}
