package com.aiplus.bi.etl.output.rdb;

import com.aiplus.bi.etl.ConfigurableJob;
import com.aiplus.bi.etl.JobTools;
import com.aiplus.bi.etl.MetadataConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.UUID;

/**
 * 将Hive的表导出到RDMBS里面，目前仅支持单表导出.
 *
 * @author dev
 */
public class ExportHiveToRdb extends ConfigurableJob {

    private static final Log LOG = LogFactory.getLog(ExportHiveToRdb.class);

    private static final String HIVE_SCHEMA_DATASOURCE_CODE = "hive_schema";

    private static final String DECIMAL_COLUMN_TYPE = "decimal";

    private static final String SQL_QUERY_HIVE_TABLE_SCHEMA = "SELECT t.TBL_NAME, c.COLUMN_NAME, c.TYPE_NAME, c.INTEGER_IDX FROM " +
            "COLUMNS_V2 c " +
            "LEFT JOIN SDS s ON s.CD_ID = c.CD_ID " +
            "LEFT JOIN TBLS t ON t.SD_ID = s.SD_ID " +
            "LEFT JOIN DBS d ON d.DB_ID = t.DB_ID " +
            "WHERE " +
            "d.`NAME` = ? AND t.TBL_NAME = ? " +
            "ORDER BY c.INTEGER_IDX ASC ";

    private String dumpSchemaName;

    private String dumpTableName;

    private String tempDumpDataDirPath;

    private MetadataConfiguration.DataSourceConnectionInfo datasourceConnectionInfo;

    protected ExportHiveToRdb() {
        super();
    }

    @Override
    public void prepared(String[] args) {
        setJobId(UUID.randomUUID().toString());
        this.dumpSchemaName = args[1];
        this.dumpTableName = args[2];
        this.tempDumpDataDirPath = "/user/hadoop/hive_dump/" + dumpTableName + "_" + getId();
        try {
            this.datasourceConnectionInfo = getMetadataConfiguration().getDataSourceConnectionInfo(args[0]);
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    @Override
    public int signal() {
        long s1 = System.currentTimeMillis();
        // Dump表创建
        dumpTableCreation();
        LOG.info("Create dump table use " + (System.currentTimeMillis() - s1) + "ms.");
        long s2 = System.currentTimeMillis();
        // 执行Hive命令
        JobTools.runCommand(getId(), new String[]{
                "hive",
                "-e",
                "insert overwrite directory '" + tempDumpDataDirPath + "' row format delimited fields terminated by ',' select * from " + dumpSchemaName + "." + dumpTableName + ";"});
        LOG.info("Dump hive data to HDFS use " + (System.currentTimeMillis() - s2) + "ms.");
        long s3 = System.currentTimeMillis();
        // 执行Sqoop Export语句
        JobTools.runCommand(getId(), new String[]{
                "sqoop",
                "export",
                "--connect",
                datasourceConnectionInfo.getUrl(),
                "--username",
                datasourceConnectionInfo.getUser(),
                "--password",
                datasourceConnectionInfo.getPassword(),
                "--table",
                dumpTableName,
                "--export-dir",
                tempDumpDataDirPath,
                "--input-fields-terminated-by",
                ",",
                "--input-null-string",
                "'\\\\N'",
                "--input-null-non-string",
                "'\\\\N'"
        });
        LOG.info("Export HDFS table data to RDBMS use " + (System.currentTimeMillis() - s3) + "ms.");
        return 0;
    }

    private void dumpTableCreation() {
        String drop = "DROP TABLE IF EXISTS `" + dumpTableName + "`;";
        StringBuilder create = new StringBuilder("CREATE TABLE IF NOT EXISTS `").append(dumpTableName).append("` (");
        LOG.info("Query table schema SQL: " + SQL_QUERY_HIVE_TABLE_SCHEMA);
        // 先查询Hive中的Schema
        try (Connection conn = getMetadataConfiguration().getDatasourceConnection(HIVE_SCHEMA_DATASOURCE_CODE); PreparedStatement pstmt = conn.prepareStatement(SQL_QUERY_HIVE_TABLE_SCHEMA)) {
            pstmt.setString(1, dumpSchemaName);
            LOG.info("Query table schema parameter 1: " + dumpSchemaName);
            pstmt.setString(2, dumpTableName);
            LOG.info("Query table schema parameter 2: " + dumpTableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String column = rs.getString("COLUMN_NAME");
                    String dataType = convertToDbType(rs.getString("TYPE_NAME"));
                    create.append(column).append(" ").append(dataType).append(",");
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
        create.trimToSize();
        create.deleteCharAt(create.length() - 1);
        create.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
        LOG.info("Prepare create dump table......");
        // 执行dump语句
        try (Connection conn = getMetadataConfiguration().getDatasourceConnection(datasourceConnectionInfo); Statement stmt = conn.createStatement()) {
            stmt.execute(drop);
            LOG.info("Drop table: " + drop);
            stmt.execute(create.toString());
            LOG.info("Create table: " + create.toString());
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private String convertToDbType(String dataType) {
        if (dataType.toLowerCase().startsWith(DECIMAL_COLUMN_TYPE)) {
            return dataType.toUpperCase();
        }
        switch (dataType.toLowerCase()) {
            case "string":
                return "VARCHAR(200)";
            case "bigint":
                return "BIGINT(20)";
            case "int":
            case "tinyint":
                return "INT(11)";
            case "timestamp":
            case "date":
                return "DATETIME";
            default:
                return "VARCHAR(200)";
        }
    }
}
