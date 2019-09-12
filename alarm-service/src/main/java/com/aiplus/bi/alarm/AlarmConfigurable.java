package com.aiplus.bi.alarm;

/**
 * @author dev
 */
public interface AlarmConfigurable {

    String ALARM_HTTP_PORT = "alarm.http.port";

    String ALARM_MAIL_SMTP_HOST = "alarm.mail.smtp.host";

    String ALARM_MAIL_SMTP_PORT = "alarm.mail.smtp.port";

    String ALARM_MAIL_SENDER = "alarm.mail.sender";

    String ALARM_MAIL_USER = "alarm.mail.user";

    String ALARM_MAIL_PWD = "alarm.mail.password";

    String ALARM_MAIL_TLS = "alarm.mail.tls";

    String ALARM_MAIL_USERAUTH = "alarm.mail.useAuth";

    String ALARM_MAIL_ERROR_CONTINUE_DOWNLOAD_REMOTE_FILE = "alarm.mail.error.continue.downloadRemoteFile";

    String ALARM_MAIL_BODY_FORMAT_FILE = "alarm.mail.body.format.file";

    String ALARM_JSCH_KNOWN_HOST = "alarm.jsch.known.host";

    String ALARM_JSCH_IDENTITY = "alarm.jsch.identity";

    String ALARM_JSCH_KNOWN_USER = "alarm.jsch.known.user";

    String ALARM_JSCH_KNOWN_HOST_PORT = "alarm.jsch.known.port";

    String ALARM_JSCH_SESSION_TIMEOUT = "alarm.jsch.session.timeout";

    String ALARM_JSCH_CHANNEL_TIMEOUT = "alarm.jsch.channel.timeout";

    String ALARM_TMP_DIR = "alarm.tmp.dir";

    String ALARM_ENGINE_TYPE = "alarm.engine.type";

    String UNKNOWN = "UNKNOWN";
}
