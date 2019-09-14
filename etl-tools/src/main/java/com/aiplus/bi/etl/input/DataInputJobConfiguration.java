package com.aiplus.bi.etl.input;

import java.io.Serializable;

/**
 * @author dev
 */
public class DataInputJobConfiguration implements Serializable {

    /**
     * 任务的名称,可以为空
     */
    private String jobName;

    /**
     * 配置是否检查schema，如果为True，则源数据的Schema必须要和目标数据的完全一致。
     */
    private boolean checkSchema = true;

    /**
     * 源数据库
     */
    private DataSource source;

    /**
     * 保存schema的数据库
     */
    private DataSource schema;

    /**
     * 任务数量（进行数据同步的任务数量）
     */
    private int taskNum;

    /**
     * 表的映射关系：源 -> 目标
     */
    private TableMapper[] mappers;

    /**
     * 输出配置
     */
    private Output output;

    public DataSource getSource() {
        return source;
    }

    public void setSource(DataSource source) {
        this.source = source;
    }

    public DataSource getSchema() {
        return schema;
    }

    public void setSchema(DataSource schema) {
        this.schema = schema;
    }

    public boolean isCheckSchema() {
        return checkSchema;
    }

    public void setCheckSchema(boolean checkSchema) {
        this.checkSchema = checkSchema;
    }

    public TableMapper[] getMappers() {
        return mappers;
    }

    public void setMappers(TableMapper[] mappers) {
        this.mappers = mappers;
    }

    public int getTaskNum() {
        return taskNum;
    }

    public void setTaskNum(int taskNum) {
        this.taskNum = taskNum;
    }

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public static class DataSource {
        private String type;
        private String url;
        private String username;
        private String password;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    public static class TableMapper {
        private String sourceTable;
        private String splitKey;
        private TableFieldMapper source;
        private String targetTable;

        public String getSourceTable() {
            return sourceTable;
        }

        public void setSourceTable(String sourceTable) {
            this.sourceTable = sourceTable;
        }

        public TableFieldMapper getSource() {
            return source;
        }

        public void setSource(TableFieldMapper source) {
            this.source = source;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public void setTargetTable(String targetTable) {
            this.targetTable = targetTable;
        }

        public String getSplitKey() {
            return splitKey;
        }

        public void setSplitKey(String splitKey) {
            this.splitKey = splitKey;
        }
    }

    public static class TableFieldMapper {
        private String tableName;
        private String[] fields;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String[] getFields() {
            return fields;
        }

        public void setFields(String[] fields) {
            this.fields = fields;
        }
    }

    public static class Output {
        private DataInputs.Type type;
        /**
         * 数据输出的路径（目前就是文件路径），正常的输出，每个表都会对应一个文件路径.
         * 此配置表示数据输出的基本路径，多表的情况下，在此路径下以表名称建子目录进行数据存放.
         */
        private String path;
        /**
         * 是否需要创建表，仅适用于Type=hive的情况
         */
        private boolean createHiveTable;
        /**
         * Hive外表的路径，不是必须，如果有则建一个外表
         */
        private String hiveExternalPath;

        /**
         * Hive的数据库名字
         */
        private String hiveSchema;

        public DataInputs.Type getType() {
            return type;
        }

        public void setType(DataInputs.Type type) {
            this.type = type;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public boolean isCreateHiveTable() {
            return createHiveTable;
        }

        public void setCreateHiveTable(boolean createHiveTable) {
            this.createHiveTable = createHiveTable;
        }

        public String getHiveExternalPath() {
            return hiveExternalPath;
        }

        public void setHiveExternalPath(String hiveExternalPath) {
            this.hiveExternalPath = hiveExternalPath;
        }

        public String getHiveSchema() {
            return hiveSchema;
        }

        public void setHiveSchema(String hiveSchema) {
            this.hiveSchema = hiveSchema;
        }
    }
}
