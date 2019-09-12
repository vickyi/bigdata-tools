package com.aiplus.bi.alarm;

import com.aiplus.bi.log.JSchSLF4JLogger;
import com.jcraft.jsch.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class JSchTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(JSchTest.class);

    @Before
    public void init() {
        JSchSLF4JLogger.initJSchLogger();
    }

    @Test
    public void testSystemProperties() {
        Properties props = System.getProperties();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
    }

    @Test
    public void testSFtpList() {
        JSch jSch = new JSch();

        Session session = null;
        ChannelSftp channel = null;
        try {
            // set known host when non-use password to login.
            jSch.setKnownHosts("/Users/dev/.ssh/known_hosts");
            jSch.addIdentity("/Users/dev/.ssh/id_rsa");
            session = jSch.getSession("dev",
                    "localhost",
                    22);
            // set first login tips, exp: ask | yes | no
            session.setConfig("StrictHostKeyChecking", "no");
            // connect session with timeout.
            session.connect(3000);
            LOGGER.info("SFTP session is connected. Open channel next...");
            // create sftp channel
            channel = (ChannelSftp) session.openChannel("sftp");
            // connect channel with timeout.
            channel.connect(30000);
            LOGGER.info("SFTP channel is connected. ");
            for (Object l : channel.ls("/Users/dev/Downloads")) {
                System.out.println(l);
            }
            // mkdir a tmp dir for download files.
        } catch (JSchException | SftpException e) {
            LOGGER.error("Get remote file error.", e);
        } finally {
            if (null != channel) {
                channel.disconnect();
            }
            if (null != session) {
                session.disconnect();
            }
            LOGGER.info("Closed channel and session successful...");
        }
    }
}
