package com.aiplus.bi.alarm.core;

/**
 * 告警引擎.
 *
 * @author dev
 */
public interface AlarmEngine {

    /**
     * 接收告警请求，生成告警执行器. 同一个引擎可以针对不同的请求生成不同的执行器.
     *
     * @param request 告警请求
     * @return 告警执行器
     */
    Executor accept(AlarmRequest request);

    /**
     * 告警执行器，具体执行告警的地方.
     */
    interface Executor {
        /**
         * 执行告警操作.
         *
         * @return 告警结果
         * @throws AlarmException 告警过程中发生不可抗力的异常信息
         */
        AlarmResponse execute() throws AlarmException;
    }
}
