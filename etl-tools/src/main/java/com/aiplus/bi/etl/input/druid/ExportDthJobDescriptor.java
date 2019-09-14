package com.aiplus.bi.etl.input.druid;

import com.aiplus.bi.etl.ConfigurableJob;
import com.aiplus.bi.etl.JobDescriptor;

/**
 * @author dev
 */
public class ExportDthJobDescriptor implements JobDescriptor {
    @Override
    public String getJobName() {
        return "export-dth";
    }

    @Override
    public ConfigurableJob newJob() {
        return new ExportDruidToHadoop();
    }
}
