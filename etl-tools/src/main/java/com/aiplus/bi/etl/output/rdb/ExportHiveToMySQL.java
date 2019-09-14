package com.aiplus.bi.etl.output.rdb;

import com.aiplus.bi.etl.ConfigurableJob;
import com.aiplus.bi.etl.JobTools;
import com.aiplus.bi.etl.MetadataConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

/**
 * 将Hive的表导出到RDMBS里面
 *
 * @author zhangjq
 */
public class ExportHiveToMySQL extends ConfigurableJob {

    private static final Log LOG = LogFactory.getLog(ExportHiveToMySQL.class);

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

    private String targetTableName;

    private String sourceTableName;

    private String dataDate;

    private String tempDumpDataDirPath;

    private String hiveDb;
    private String hiveTable;
    private String hiveSql;
    private String mysqlSource;
    private String mysqlCleanSql;
    private String mysqlTable;

    private MetadataConfiguration.DataSourceConnectionInfo datasourceConnectionInfo;

    protected ExportHiveToMySQL() {
        super();
    }

    /**
     * 从properties文件解析几个参数：
     * hive的库表：hive.db
     * 以及 hive.table
     * 从 hive 表查询数据的语句: hive.sql
     * mysql库表：mysql.db
     * 以及 mysql.table
     * 清理 mysql 数据的 sql: mysql.clean.sql
     */
    private void getProp(String dumpConf) {
        Properties properties = new Properties();
        File file = new File(dumpConf);
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            properties.load(fis);

            this.hiveDb = properties.getProperty("hive.db");
            this.hiveTable = properties.getProperty("hive.table");
            this.hiveSql = properties.getProperty("hive.sql");
            this.mysqlSource = properties.getProperty("mysql.source");
            this.mysqlCleanSql = properties.getProperty("mysql.clean_sql");
            this.mysqlTable = properties.getProperty("mysql.table");

            fis.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void prepared(String[] args) {
        setJobId(UUID.randomUUID().toString());

        DateTime dateTime = new DateTime();
        String yesterDay = dateTime.minusDays(1).toString("yyyy-MM-dd");

        this.dataDate = "1970-01-01".equals(args[2]) ? yesterDay : args[2];
        System.out.println("====>dataDate=" + this.dataDate);

        String dumpConf = (args[1] == null) ? "dump.properties" : args[1];

        getProp(dumpConf);

        this.tempDumpDataDirPath = "/user/hadoop/hive_dump/" + this.hiveTable + "_" + getId();

        this.hiveSql = this.hiveSql.replace("${date}", this.dataDate);

        this.mysqlCleanSql = this.mysqlCleanSql.replace("${date}", this.dataDate);

        LOG.info("======>> prepared hiveDb: " + this.hiveDb);
        LOG.info("======>> prepared hiveTable: " + this.hiveTable);
        LOG.info("======>> prepared hiveSql: " + this.hiveSql);
        LOG.info("======>> prepared mysqlSource: " + this.mysqlSource);
        LOG.info("======>> prepared mysqlCleanSql: " + this.mysqlCleanSql);
        LOG.info("======>> prepared mysqlTable: " + this.mysqlTable);
        LOG.info("======>> prepared tempDumpDataDirPath: " + this.tempDumpDataDirPath);

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
        // 删除数据，避免数据重复
        cleanTableData();
        LOG.info("======>> Clean target table cost " + (System.currentTimeMillis() - s1) + "ms.");

        long s2 = System.currentTimeMillis();
        // 执行Hive命令
        String hiveSqlStr = "set mapreduce.job.queuename=bi;insert overwrite directory "
                + "'" + tempDumpDataDirPath + "'"
                + " row format delimited fields terminated by ',' NULL DEFINED AS '' " + this.hiveSql + ";";
        LOG.info("======>> prepared hiveSqlStr: " + hiveSqlStr);

        JobTools.runCommand(getId(), new String[]{
                "hive",
                "-e",
                hiveSqlStr
        });

        LOG.info("======>> Dump hive data to HDFS use " + (System.currentTimeMillis() - s2) + "ms.");

        // 执行Sqoop Export语句
        String[] sqoopCmd = new String[]{
                "sqoop",
                "export",
                "-D",
                "mapreduce.job.queuename=bi",
                "--connect",
                datasourceConnectionInfo.getUrl(),
                "--username",
                datasourceConnectionInfo.getUser(),
                "--password",
                datasourceConnectionInfo.getPassword(),
                "--table",
                mysqlTable,
                "--export-dir",
                tempDumpDataDirPath,
                "--input-fields-terminated-by",
                ",",
                "--input-null-string",
                "'\\\\N'",
                "--input-null-non-string",
                "'\\\\N'"
        };

        LOG.info("======>> sqoop Cmd ready: " + Arrays.toString(sqoopCmd));

        long s3 = System.currentTimeMillis();

        JobTools.runCommand(getId(), sqoopCmd);

        LOG.info("======>> Export HDFS table data to RDBMS use " + (System.currentTimeMillis() - s3) + "ms.");

        return 0;
    }

    private void cleanTableData() {
        LOG.info("======>> Prepare clean target table, mysqlCleanSql: " + mysqlCleanSql);
        // 执行dump语句
        try (Connection conn = getMetadataConfiguration().getDatasourceConnection(datasourceConnectionInfo); Statement stmt = conn.createStatement()) {
            stmt.execute(mysqlCleanSql);
            LOG.info("======>> Clean target table finish: " + mysqlCleanSql);
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
