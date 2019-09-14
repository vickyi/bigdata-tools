package com.aiplus.bi.etl.output.rdb;

import com.aiplus.bi.etl.ConfigurableJob;
import com.aiplus.bi.etl.JobDescriptor;

/**
 * 导出Hive的单表的数据到RDB数据库的单表中.
 *
 * @author dev
 */
public class ExportHive2MySQLDescriptor implements JobDescriptor {

    @Override
    public String getJobName() {
        return "hive2mysql";
    }

    @Override
    public ConfigurableJob newJob() {
        return new ExportHiveToMySQL();
    }
}
