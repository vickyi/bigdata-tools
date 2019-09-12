package com.aiplus.bi.alarm.core;

public class AlarmException extends Exception {

    public AlarmException() {
        super();
    }

    public AlarmException(String message) {
        super(message);
    }

    public AlarmException(String message, Throwable cause) {
        super(message, cause);
    }

    public AlarmException(Throwable cause) {
        super(cause);
    }

    protected AlarmException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
