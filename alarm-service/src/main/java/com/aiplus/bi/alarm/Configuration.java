package com.aiplus.bi.alarm;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author dev
 */
public class Configuration implements AlarmConfigurable {

    private static final String ETC_FILE = "alarm-etc.properties";

    private static final int DEFAULT_MAIL_SMTP_PORT = 25;

    private static final int DEFAULT_HTTP_PORT = 9998;

    private Properties props = new Properties();

    public void loadFromClasspath() {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(ETC_FILE);
        loadFromInputStream(in);
    }

    public void loadFromFile(String file) {
        try {
            loadFromInputStream(new FileInputStream(new File(file, ETC_FILE)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadFromInputStream(InputStream in) {
        try {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException ignored) {
            }
        }
    }

    public int getAlarmHttpPort() {
        return getInt(ALARM_HTTP_PORT, DEFAULT_HTTP_PORT);
    }

    public String getAlarmMailSmtpHost() {
        return props.getProperty(ALARM_MAIL_SMTP_HOST);
    }

    public int getAlarmMailSmtpPort() {
        return getInt(ALARM_MAIL_SMTP_PORT, DEFAULT_MAIL_SMTP_PORT);
    }

    public String getAlarmMailSender() {
        return get(ALARM_MAIL_SENDER, null);
    }

    public String getAlarmMailUser() {
        return get(ALARM_MAIL_USER, null);
    }

    public String getAlarmMailPassword() {
        return get(ALARM_MAIL_PWD, null);
    }

    public String getAlarmMailTls() {
        return get(ALARM_MAIL_TLS, "false");
    }

    public boolean isAlarmMailUseAuth() {
        return getBoolean(ALARM_MAIL_USERAUTH, true);
    }

    public int getInt(String key, int def) {
        String val = get(key, null);
        return null == val ? def : Integer.parseInt(val);
    }

    public boolean getBoolean(String key, boolean def) {
        String val = get(key, null);
        return null == val ? def : Boolean.parseBoolean(val);
    }

    public Map<String, String> all() {
        Map<String, String> result = new HashMap<>(props.size());
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            result.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return result;
    }

    public String get(String key, String def) {
        return props.getProperty(key, def);
    }

    public void set(String key, String val) {
        props.setProperty(key, val);
    }
}
