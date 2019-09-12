package com.aiplus.bi.etl.input;

/**
 * Hadoop工具可配置类.
 *
 * @author dev
 */
public interface HadoopToolConfigurable {

    /**
     * Hadoop工具的配置前缀.
     */
    String HADOOP_TOOL_CONF_PREFIX = "hadoop.tool.";

    /**
     * Hadoop工具配置参数的前缀
     */
    String HADOOP_TOOL_CONF_D = HADOOP_TOOL_CONF_PREFIX + "d.";
}
