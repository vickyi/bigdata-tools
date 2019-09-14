package com.aiplus.bi.etl;

/**
 * Job的描述器.
 *
 * @author dev
 */
public interface JobDescriptor {

    /**
     * 获取Job的名称，这个名称就是在运行的时候指定运行Job的名称.
     *
     * @return 名称
     */
    String getJobName();

    /**
     * 创建一个新的Job对象.
     *
     * @return Job对象
     */
    ConfigurableJob newJob();
}
