package com.aiplus.bi.log;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Logger;
import org.slf4j.LoggerFactory;

public class JSchSLF4JLogger implements Logger {

    private static final String JSCH_LOGGER_NAME = JSchSLF4JLogger.class.getName();
    private final org.slf4j.Logger logger;

    public JSchSLF4JLogger(String name) {
        super();
        logger = LoggerFactory.getLogger(name);
    }

    public static void initJSchLogger() {
        JSch.setLogger(new JSchSLF4JLogger(JSCH_LOGGER_NAME));
    }

    @Override
    public boolean isEnabled(int level) {
        switch (level) {
            case DEBUG:
                return logger.isDebugEnabled();
            case INFO:
                return logger.isInfoEnabled();
            case ERROR:
                return logger.isErrorEnabled();
            case FATAL:
                return logger.isTraceEnabled();
            case WARN:
                return logger.isWarnEnabled();
            default:
                return false;
        }
    }

    @Override
    public void log(int level, String message) {
        message = Thread.currentThread().getStackTrace()[2] + " => " + message;
        switch (level) {
            case Logger.DEBUG:
                logger.debug(message);
                break;
            case Logger.ERROR:
                logger.error(message);
                break;
            case Logger.FATAL:
                logger.trace(message);
                break;
            case Logger.INFO:
                logger.info(message);
                break;
            case Logger.WARN:
                logger.warn(message);
                break;
            default:
        }
    }
}
