package com.aiplus.bi.etl.input.druid;

import com.aiplus.bi.etl.input.HadoopToolConfigurable;

public interface DruidInputConfigurable extends HadoopToolConfigurable {

    String DRUID_DATA_LOCAL_DIR = "etl.job.druid.data.local.dir";

    String DRUID_JOB_ARGS_EXPORT_DATASOURCE = "etl.job.druid.args.export.datasource";

    String DRUID_JOB_ARGS_EXPORT_DATE = "etl.job.druid.args.export.date";
}
