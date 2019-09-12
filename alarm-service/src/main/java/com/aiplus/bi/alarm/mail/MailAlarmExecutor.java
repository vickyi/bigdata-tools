package com.aiplus.bi.alarm.mail;

import com.aiplus.bi.alarm.Configuration;
import com.aiplus.bi.alarm.core.AlarmEngine;
import com.aiplus.bi.alarm.core.AlarmException;
import com.aiplus.bi.alarm.core.AlarmRequest;
import com.aiplus.bi.alarm.core.AlarmResponse;

import javax.mail.MessagingException;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * 简单的发送邮件消息告警的执行器.
 *
 * @author dev
 */
public class MailAlarmExecutor implements AlarmEngine.Executor {

    private final AlarmRequest request;

    private final EmailMessageCreator emailMessageCreator;

    private final Configuration conf;

    public MailAlarmExecutor(AlarmRequest request, EmailMessageCreator emailMessageCreator, Configuration conf) {
        this.request = request;
        this.emailMessageCreator = emailMessageCreator;
        this.conf = conf;
    }

    @Override
    public AlarmResponse execute() throws AlarmException {
        if (null == this.request) {
            throw new AlarmException("Please do accept method when process a alarm.");
        }
        return sendMailMessage(request.getBody());
    }

    protected AlarmResponse sendMailMessage(String body) throws AlarmException {
        return sendMailMessage(body, null);
    }

    protected AlarmResponse sendMailMessage(String body, Map<String, File> attachments) throws AlarmException {
        long start = System.currentTimeMillis();
        // then start send mail message. Ignore body format property.
        EmailMessage message = emailMessageCreator.createMessage();
        String subject = "[" + request.getLevel().toUpperCase() + "] " + request.getSubject();
        message.setSubject(subject);
        message.setBody(body, EmailMessage.MIME_TYPE_TEXT_HTML);
        message.addAllToAddress(new ArrayList<>(Arrays.asList(request.getToAddress())));
        try {
            if (null != attachments && !attachments.isEmpty()) {
                for (Map.Entry<String, File> entry : attachments.entrySet()) {
                    message.addAttachment(entry.getKey(), entry.getValue());
                }
            }
            message.sendEmail();
        } catch (MessagingException e) {
            throw new AlarmException("Mail send exception.", e);
        }
        return AlarmResponse.ok(start).message("Send mail successful.");
    }

    public AlarmRequest getRequest() {
        return request;
    }

    public EmailMessageCreator getEmailMessageCreator() {
        return emailMessageCreator;
    }

    public Configuration getConf() {
        return conf;
    }
}
