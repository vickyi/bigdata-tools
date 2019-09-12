package com.aiplus.bi.alarm.core;

import java.io.Serializable;
import java.util.Date;

/**
 * 告警响应结果.
 *
 * @author dev
 */
public class AlarmResponse implements Serializable {

    public static final String CODE_SUCCESSFUL = "00000";

    public static final String CODE_FAILURE = "99999";

    private String code;

    private String message;

    private long duration;

    private Date doneTime;

    private AlarmResponse(String code, long start) {
        Date now = new Date();
        this.code = code;
        this.duration = now.getTime() - start;
        this.doneTime = now;
    }

    public static AlarmResponse ok(long start) {
        return create(CODE_SUCCESSFUL, start);
    }

    public static AlarmResponse failure(long start) {
        return create(CODE_FAILURE, start);
    }

    public static AlarmResponse create(String code, long start) {
        return new AlarmResponse(code, start);
    }

    public AlarmResponse message(String message) {
        this.message = message;
        return this;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public long getDuration() {
        return duration;
    }

    public Date getDoneTime() {
        return doneTime;
    }
}
