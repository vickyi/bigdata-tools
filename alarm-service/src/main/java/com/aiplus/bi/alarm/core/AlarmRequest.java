package com.aiplus.bi.alarm.core;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * 一个简单的告警请求.
 *
 * @author dev
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "message", value = AlarmRequest.class),
        @JsonSubTypes.Type(name = "remote_file", value = RemoteFileAlarmRequest.class),
})
public class AlarmRequest implements Serializable {

    /**
     * 告警的类型：message（消息形式）, remote_file（远程文件内容）.
     */
    private String type = "message";

    /**
     * 发送告警的用户.
     */
    private String user;

    /**
     * 发送告警的地址.
     */
    private String host;

    /**
     * 警告的级别：FATAL, DEBUG, INFO, WARN, ERROR.
     */
    private String level;

    /**
     * 报警的主题.
     */
    private String subject;

    /**
     * 报警的内容.
     */
    private String body;

    /**
     * 送达地址.可以是多个.
     */
    private String[] toAddress;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String[] getToAddress() {
        return toAddress;
    }

    public void setToAddress(String[] toAddress) {
        this.toAddress = toAddress;
    }

}
