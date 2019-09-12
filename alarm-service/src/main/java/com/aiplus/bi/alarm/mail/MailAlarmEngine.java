package com.aiplus.bi.alarm.mail;

import com.aiplus.bi.alarm.Configuration;
import com.aiplus.bi.alarm.core.AlarmEngine;
import com.aiplus.bi.alarm.core.AlarmRequest;

/**
 * @author dev
 */
public class MailAlarmEngine implements AlarmEngine {

    private Configuration conf;

    private EmailMessageCreator emailMessageCreator;

    public MailAlarmEngine(Configuration conf) {
        this.conf = conf;
        this.emailMessageCreator = new EmailMessageCreator(conf);
    }

    @Override
    public AlarmEngine.Executor accept(final AlarmRequest request) {
        AlarmEngine.Executor executor;
        switch (request.getType()) {
            case "remote_file": {
                executor = new RemoteFileMailAlarmExecutor(request, emailMessageCreator, conf);
                break;
            }
            default: {
                // if request type is message, then current send message text to other
                executor = new MailAlarmExecutor(request, emailMessageCreator, conf);
            }
        }
        return executor;
    }
}
