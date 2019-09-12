package com.aiplus.bi.etl;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Job的注册机，这个里面初始化了很多Job的描述器，根据Job的描述器去创建一个可以运行的Job.
 *
 * @author dev
 */
public class JobRegistry {

    private Map<String, JobDescriptor> jobDescriptorMap = new HashMap<>(10);

    private JobRegistry() {
        for (JobDescriptor jobDescriptor : ServiceLoader.load(JobDescriptor.class)) {
            jobDescriptorMap.put(jobDescriptor.getJobName(), jobDescriptor);
        }
    }

    public static JobRegistry getInstance() {
        return Holder.INSTANCE;
    }

    public ConfigurableJob createRegisterJob(String registerName) {
        if (!jobDescriptorMap.containsKey(registerName)) {
            throw new NullPointerException("Not found register job with name: " + registerName);
        }
        return jobDescriptorMap.get(registerName).newJob();
    }

    private static final class Holder {
        private static final JobRegistry INSTANCE = new JobRegistry();
    }
}
