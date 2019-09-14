package com.aiplus.bi.etl;

public interface Job {

    /**
     * 获取Job的唯一标识.
     *
     * @return id.
     */
    String getId();

    /**
     * 准备Job，解析或设置一些参数.
     *
     * @param args 参数
     */
    void prepared(String[] args);

    /**
     * 发起启动Job的信号.
     *
     * @return 启动结果
     */
    int signal();
}
