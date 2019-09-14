package com.aiplus.bi.etl.input.rdb;

import com.aiplus.bi.etl.input.DataInputJobConfiguration;
import com.aiplus.bi.etl.input.ExportDataToHadoop;
import com.aiplus.bi.etl.input.MapReduceJob;

/**
 * 导出RDB的数据到Hadoop平台（HDFS/Hive）.
 *
 * @author dev
 */
public class ExportRdbToHadoop extends ExportDataToHadoop {

    @Override
    protected MapReduceJob createDataInputJob(String jobId, DataInputJobConfiguration jobConfiguration) {
        return new RdbDataInputJob(jobId, jobConfiguration);
    }
}
