package com.aiplus.bi.alarm.core;

import com.aiplus.bi.alarm.AlarmConfigurable;
import com.aiplus.bi.alarm.Configuration;
import com.aiplus.bi.alarm.mail.MailAlarmEngine;

/**
 * @author dev
 */
public class AlarmEngineFactory implements AlarmConfigurable {

    private AlarmEngineFactory() {
    }

    public static AlarmEngineFactory getInstance() {
        return Holder.INSTANCE;
    }

    public AlarmEngine createEngine(Configuration conf) {
        String alarmType = conf.get(AlarmConfigurable.ALARM_ENGINE_TYPE, AlarmConfigurable.UNKNOWN);
        AlarmEngine engine;
        switch (alarmType.toUpperCase()) {
            case "MAIL": {
                engine = new MailAlarmEngine(conf);
                break;
            }
            default: {
                throw new IllegalArgumentException("Unknown alarm[" + alarmType + "]!");
            }
        }
        return engine;
    }

    private static final class Holder {
        private static final AlarmEngineFactory INSTANCE = new AlarmEngineFactory();
    }
}
