package com.aiplus.bi.etl.input;

import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataInputs {

    public static final String DATA_INPUT_JOB_CONFIGURATION_PROPERTY = "etl.job.configuration";
    public static final String DATA_INPUT_JOB_ID = "etl.job.id";
    public static final String DATA_INPUT_JOB_NAME = "etl.job.name";
    private Configuration conf;
    private DataInputJobConfiguration jobConfiguration;

    public DataInputs(Configuration conf) {
        this.conf = conf;
        String json = conf.get(DATA_INPUT_JOB_CONFIGURATION_PROPERTY);
        if (null == json) {
            throw new IllegalArgumentException("Data input job configuration not found!");
        }
        this.jobConfiguration = new GsonBuilder().serializeNulls().create().fromJson(json, DataInputJobConfiguration.class);
    }

    public static void configureDataInputJob(Configuration conf, String jobId, String jobName, String jobConfigurationContent) {
        conf.set(DATA_INPUT_JOB_ID, jobId);
        conf.set(DATA_INPUT_JOB_NAME, jobName);
        conf.set(DATA_INPUT_JOB_CONFIGURATION_PROPERTY, jobConfigurationContent);
    }

    public Configuration getMapReduceConfiguration() {
        return this.conf;
    }

    public DataInputJobConfiguration getJobConfiguration() {
        return this.jobConfiguration;
    }

    public synchronized Connection getSourceConnection() throws SQLException {
        if (this.jobConfiguration.getSource().getUsername() == null) {
            return DriverManager.getConnection(this.jobConfiguration.getSource().getUrl());
        } else {
            return DriverManager.getConnection(
                    this.jobConfiguration.getSource().getUrl(),
                    this.jobConfiguration.getSource().getUsername(),
                    this.jobConfiguration.getSource().getPassword()
            );
        }
    }

    public enum Type {
        /**
         * 文件类型
         */
        FILE,

        /**
         * Hive表类型
         */
        HIVE
    }
}
