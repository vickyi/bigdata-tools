package com.aiplus.bi.etl.input.rdb;

import com.aiplus.bi.etl.ConfigurableJob;
import com.aiplus.bi.etl.JobDescriptor;

/**
 * 导出RDB的数据到Hadoop平台的Job描述类.
 *
 * @author dev
 */
public class ExportRthJobDescriptor implements JobDescriptor {

    @Override
    public String getJobName() {
        return "export-rth";
    }

    @Override
    public ConfigurableJob newJob() {
        return new ExportRdbToHadoop();
    }
}
