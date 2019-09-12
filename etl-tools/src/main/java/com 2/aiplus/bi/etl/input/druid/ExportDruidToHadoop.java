package com.aiplus.bi.etl.input.druid;

import com.aiplus.bi.etl.input.DataInputJobConfiguration;
import com.aiplus.bi.etl.input.ExportDataToHadoop;
import com.aiplus.bi.etl.input.MapReduceJob;

/**
 * @author dev
 */
public class ExportDruidToHadoop extends ExportDataToHadoop {

    @Override
    protected MapReduceJob createDataInputJob(String jobId, DataInputJobConfiguration jobConfiguration) {
        return new DruidDataInputJob(jobId, jobConfiguration);
    }
}
