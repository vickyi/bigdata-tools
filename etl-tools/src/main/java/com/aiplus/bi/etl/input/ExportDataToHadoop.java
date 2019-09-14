package com.aiplus.bi.etl.input;

import com.aiplus.bi.etl.ConfigurableJob;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

/**
 * 导出数据到Hadoop平台（HDFS/Hive）.
 *
 * @author dev
 */
public abstract class ExportDataToHadoop extends ConfigurableJob implements HadoopToolConfigurable {

    private static final Log LOG = LogFactory.getLog(ExportDataToHadoop.class);

    private static final String SQL_QUERY_ETL_JOB = "SELECT " +
            "T1.ID, T1.DEFINE_NAME, T1.INPUT_SOURCE_ID, T1.OUTPUT_SOURCE_ID, T1.TASK_NUM, T1.TABLE_MAPPERS, " +
            "T2.SOURCE_CODE, T2.SOURCE_URL, T2.SOURCE_USER, T2.SOURCE_PASSWORD, T2.SOURCE_TYPE,  " +
            "T3.OBJECT_TYPE, T3.DATA_PATH, T3.CREATE_HIVE_TABLE, T3.HIVE_EXTERNAL_TABLE, T3.HIVE_SCHEMA, T3.OUTPUT_ENV " +
            "FROM BI_ETL_JOB_DEFINE T1, BI_DATASOURCE T2, BI_HADOOP_OUTPUT T3 " +
            "WHERE " +
            "T1.INPUT_SOURCE_ID = T2.ID AND T1.OUTPUT_SOURCE_ID = T3.ID AND " +
            "T1.OUTPUT_TYPE = 'HADOOP' AND T1.DEFINE_NAME = ?";

    private String[] jobArgs;

    private String jobDefineName;

    private String hadoopClusterConfPath;

    @Override
    public void prepared(String[] args) {
        setJobId(UUID.randomUUID().toString());
        this.jobDefineName = args[0];
        parseHadoopToolConfiguration(args);
    }

    @Override
    public int signal() {
        // 初始化ETL Job Configuration对象
        DataInputJobConfiguration jobConfiguration = initDataInputJobConfiguration();
        Configuration conf = loadHadoopClusterConfiguration();
        try {
            return ToolRunner.run(conf, createDataInputJob(getId(), jobConfiguration), jobArgs);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    protected abstract MapReduceJob createDataInputJob(String jobId, DataInputJobConfiguration jobConfiguration);

    private DataInputJobConfiguration initDataInputJobConfiguration() {
        // 查询对应Job的配置信息
        try (Connection conn = getMetadataConfiguration().getMetadataConnection();
             PreparedStatement pstmt = conn.prepareStatement(SQL_QUERY_ETL_JOB)) {
            pstmt.setString(1, jobDefineName);

            LOG.info("Query job[" + jobDefineName + "] configuration: " + SQL_QUERY_ETL_JOB);

            DataInputJobConfiguration jobConfiguration = new DataInputJobConfiguration();
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    LOG.info("Find job [" + jobDefineName + "] configuration.");
                    // hadoop cluster conf path.
                    hadoopClusterConfPath = rs.getString("OUTPUT_ENV");
                    // etl job configuration
                    jobConfiguration.setJobName(rs.getString("DEFINE_NAME"));
                    jobConfiguration.setTaskNum(rs.getInt("TASK_NUM"));
                    // source
                    DataInputJobConfiguration.DataSource dataSource = new DataInputJobConfiguration.DataSource();
                    dataSource.setType(rs.getString("SOURCE_TYPE"));
                    dataSource.setUrl(rs.getString("SOURCE_URL"));
                    dataSource.setUsername(rs.getString("SOURCE_USER"));
                    dataSource.setPassword(rs.getString("SOURCE_PASSWORD"));
                    jobConfiguration.setSource(dataSource);
                    // output
                    DataInputJobConfiguration.Output output = new DataInputJobConfiguration.Output();
                    output.setType(DataInputs.Type.valueOf(rs.getString("OBJECT_TYPE").toUpperCase()));
                    output.setPath(rs.getString("DATA_PATH"));
                    output.setCreateHiveTable(rs.getInt("CREATE_HIVE_TABLE") == 1);
                    output.setHiveExternalPath(rs.getString("HIVE_EXTERNAL_TABLE"));
                    output.setHiveSchema(rs.getString("HIVE_SCHEMA"));
                    jobConfiguration.setOutput(output);
                    // table mappers
                    String strTableMappersJson = rs.getString("TABLE_MAPPERS");
                    JsonArray tableMapperJsonArray = new GsonBuilder().serializeNulls().create().fromJson(strTableMappersJson, JsonArray.class);
                    DataInputJobConfiguration.TableMapper[] tableMappers = new DataInputJobConfiguration.TableMapper[tableMapperJsonArray.size()];
                    // 循环初始化mapper
                    for (int j = 0; j < tableMapperJsonArray.size(); j++) {
                        DataInputJobConfiguration.TableMapper tableMapper = new DataInputJobConfiguration.TableMapper();

                        JsonElement tableMapperJsonElement = tableMapperJsonArray.get(j);
                        JsonObject jsonObject = tableMapperJsonElement.getAsJsonObject();
                        if (jsonObject.get("source").isJsonObject()) {
                            // source里面有数据
                            DataInputJobConfiguration.TableFieldMapper tableFieldMapper = new DataInputJobConfiguration.TableFieldMapper();
                            JsonObject fieldObject = jsonObject.getAsJsonObject("source");
                            JsonArray fieldArray = fieldObject.getAsJsonArray("fields");
                            String[] fields = new String[fieldArray.size()];
                            // 设置fields
                            for (int i = 0; i < fieldArray.size(); i++) {
                                fields[i] = fieldArray.get(i).getAsString();
                            }
                            tableFieldMapper.setFields(fields);
                            tableFieldMapper.setTableName(fieldObject.get("tableName").getAsString());
                            tableMapper.setSource(tableFieldMapper);
                        }
                        // 通常情况，进行常规属性赋值
                        JsonElement sourceTable = jsonObject.get("sourceTable");
                        JsonElement splitKey = jsonObject.get("splitKey");
                        JsonElement targetTable = jsonObject.get("targetTable");
                        tableMapper.setSourceTable(sourceTable.isJsonNull() ? null : sourceTable.getAsString());
                        tableMapper.setSplitKey(splitKey.isJsonNull() ? null : splitKey.getAsString());
                        tableMapper.setTargetTable(targetTable.isJsonNull() ? null : targetTable.getAsString());

                        tableMappers[j] = tableMapper;
                    }
                    jobConfiguration.setMappers(tableMappers);
                }
            }
            return jobConfiguration;
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            System.exit(-1);
        }
        return null;
    }

    private Configuration loadHadoopClusterConfiguration() {
        if (null == hadoopClusterConfPath || "".equals(hadoopClusterConfPath)) {
            return null;
        }
        LOG.info("Load hadoop cluster configuration from path: " + hadoopClusterConfPath);
        Configuration conf = new Configuration();
        // add core-site.xml, hdfs-site.xml, yarn-site.xml, mapred-site.xml
        conf.addResource(new Path(hadoopClusterConfPath + "/core-site.xml"));
        conf.addResource(new Path(hadoopClusterConfPath + "/hdfs-site.xml"));
        conf.addResource(new Path(hadoopClusterConfPath + "/yarn-site.xml"));
        conf.addResource(new Path(hadoopClusterConfPath + "/mapred-site.xml"));

        return conf;
    }

    private void parseHadoopToolConfiguration(String[] jobArgs) {
        LOG.info("Original job args: " + Arrays.toString(jobArgs));
        if (null != jobArgs && 1 < jobArgs.length) {
            String[] parsedArgs = new String[jobArgs.length - 1];
            for (int i = 1; i < jobArgs.length; i++) {
                int parseIndex = i - 1;
                String arg = jobArgs[i];
                if (!arg.startsWith(HADOOP_TOOL_CONF_PREFIX)) {
                    parsedArgs[parseIndex] = arg;
                    continue;
                }
                if (arg.startsWith(HADOOP_TOOL_CONF_D)) {
                    String dProperty = arg.substring(HADOOP_TOOL_CONF_D.length());
                    parsedArgs[parseIndex] = "-D" + dProperty;
                }
            }
            this.jobArgs = parsedArgs;
        }
        LOG.info("Job args: " + Arrays.toString(this.jobArgs));
    }
}
