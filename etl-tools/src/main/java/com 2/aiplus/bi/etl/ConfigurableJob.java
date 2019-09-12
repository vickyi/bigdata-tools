package com.aiplus.bi.etl;

import java.util.UUID;

/**
 * @author dev
 */
public abstract class ConfigurableJob implements Job, MetadataConfigurable {

    private String jobId;

    private MetadataConfiguration configuration;

    protected ConfigurableJob() {
        this.jobId = UUID.randomUUID().toString();
    }

    @Override
    public String getId() {
        return this.jobId;
    }

    @Override
    public MetadataConfiguration getMetadataConfiguration() {
        return this.configuration;
    }

    @Override
    public void setMetadataConfiguration(MetadataConfiguration configuration) {
        this.configuration = configuration;
    }

    protected void setJobId(String jobId) {
        this.jobId = jobId;
    }
}
