package com.aiplus.bi.alarm.mail;

import com.aiplus.bi.alarm.Configuration;

import javax.mail.NoSuchProviderException;
import java.util.Properties;


/**
 * @author dev
 */
public class EmailMessageCreator {

    private final String mailHost;
    private final int mailPort;
    private final String mailUser;
    private final String mailPassword;
    private final String mailSender;
    private final String tls;
    private final boolean usesAuth;

    public EmailMessageCreator(final Configuration conf) {
        this.mailHost = conf.getAlarmMailSmtpHost();
        this.mailPort = conf.getAlarmMailSmtpPort();
        this.mailUser = conf.getAlarmMailUser();
        this.mailPassword = conf.getAlarmMailPassword();
        this.mailSender = conf.getAlarmMailSender();
        this.tls = conf.getAlarmMailTls();
        this.usesAuth = conf.isAlarmMailUseAuth();
    }

    public EmailMessage createMessage() {
        final EmailMessage message = new EmailMessage(this.mailHost, this.mailPort, this.mailUser, this.mailPassword, this);
        message.setFromAddress(this.mailSender);
        message.setTLS(this.tls);
        message.setAuth(this.usesAuth);
        return message;
    }

    public JavaxMailSender createSender(final Properties props) throws NoSuchProviderException {
        return new JavaxMailSender(props);
    }
}
