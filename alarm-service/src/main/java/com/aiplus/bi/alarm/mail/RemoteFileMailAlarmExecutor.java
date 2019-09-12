package com.aiplus.bi.alarm.mail;

import com.aiplus.bi.alarm.Configuration;
import com.aiplus.bi.alarm.core.AlarmException;
import com.aiplus.bi.alarm.core.AlarmRequest;
import com.aiplus.bi.alarm.core.AlarmResponse;
import com.aiplus.bi.alarm.core.RemoteFileAlarmRequest;
import com.jcraft.jsch.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static com.aiplus.bi.alarm.AlarmConfigurable.*;

/**
 * @author dev
 */
public class RemoteFileMailAlarmExecutor extends MailAlarmExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteFileMailAlarmExecutor.class);

    private static final String REMOTE_TYPE_SFTP = "sftp";

    private static final int DEFAULT_SFTP_PORT = 22;

    private static final int DEFAULT_SFTP_SESSION_TIMEOUT = 30000;

    private static final int DEFAULT_SFTP_CHANNEL_TIMEOUT = 30000;

    private static final String NEW_LINE = "\r\n";

    public RemoteFileMailAlarmExecutor(AlarmRequest request, EmailMessageCreator emailMessageCreator, Configuration conf) {
        super(request, emailMessageCreator, conf);
    }

    @Override
    public AlarmResponse execute() throws AlarmException {
        Configuration conf = getConf();
        RemoteFileAlarmRequest request = (RemoteFileAlarmRequest) getRequest();
        RemoteFileAlarmRequest.RemoteFile[] remoteFiles = request.getRemoteFiles();
        if (null == remoteFiles || 0 == remoteFiles.length) {
            LOGGER.warn("Not found remote files from request! will be send body message only.");
            return sendMailMessage(request.getBody());
        }
        long start = System.currentTimeMillis();
        // Normal flow.
        switch (request.getRemoteType()) {
            case REMOTE_TYPE_SFTP: {
                // list files.
                Map<RemoteFileAlarmRequest.RemoteFile, String> localFiles = new HashMap<>(remoteFiles.length);
                // type if sftp, then download files with sftp.
                JSch jSch = new JSch();

                Session session = null;
                ChannelSftp channel = null;
                File tmpDir;
                try {
                    // set known host when non-use password to login.
                    jSch.setKnownHosts(conf.get(ALARM_JSCH_KNOWN_HOST, System.getProperty("user.home") + "/.ssh/known_hosts"));
                    jSch.addIdentity(conf.get(ALARM_JSCH_IDENTITY, System.getProperty("user.home") + "/.ssh/id_rsa"));
                    session = jSch.getSession(conf.get(ALARM_JSCH_KNOWN_USER, request.getUser()),
                            request.getHost(),
                            conf.getInt(ALARM_JSCH_KNOWN_HOST_PORT, DEFAULT_SFTP_PORT));
                    // set first login tips, exp: ask | yes | no
                    session.setConfig("StrictHostKeyChecking", "no");
                    // connect session with timeout.
                    session.connect(conf.getInt(ALARM_JSCH_SESSION_TIMEOUT, DEFAULT_SFTP_SESSION_TIMEOUT));
                    LOGGER.info("SFTP session is connected. Open channel next...");
                    // create sftp channel
                    channel = (ChannelSftp) session.openChannel("sftp");
                    // connect channel with timeout.
                    channel.connect(conf.getInt(ALARM_JSCH_CHANNEL_TIMEOUT, DEFAULT_SFTP_CHANNEL_TIMEOUT));
                    LOGGER.info("SFTP channel is connected. ");
                    // mkdir a tmp dir for download files.
                    String tmpDirName = UUID.randomUUID().toString();
                    String userTmpDir = conf.get(ALARM_TMP_DIR, System.getProperty("java.io.tmpdir"));
                    tmpDir = new File(userTmpDir + "/" + tmpDirName);
                    FileUtils.forceMkdir(tmpDir);
                    // downloading..
                    for (RemoteFileAlarmRequest.RemoteFile remoteFile : remoteFiles) {
                        String localFilePath = tmpDir.getAbsolutePath() + "/" + remoteFile.getName();
                        try {
                            channel.get(remoteFile.getPath(), localFilePath);
                        } catch (SftpException e) {
                            LOGGER.error("not found remote file: " + remoteFile.getPath(), e);
                            continue;
                        }
                        LOGGER.debug("Downloaded remote file: " + remoteFile.getPath() + ", save to: " + localFilePath);
                        localFiles.put(remoteFile, localFilePath);
                    }
                } catch (JSchException | IOException e) {
                    LOGGER.error("Get remote file error.", e);
                    boolean isContinueWithDownloadRemoteFileError = conf.getBoolean(ALARM_MAIL_ERROR_CONTINUE_DOWNLOAD_REMOTE_FILE, false);
                    if (isContinueWithDownloadRemoteFileError) {
                        // continue send mail message, but only request body message will be sent.
                        sendMailMessage(request.getBody());
                    }
                    return AlarmResponse.failure(start).message(e.getLocalizedMessage());
                } finally {
                    if (null != channel) {
                        channel.disconnect();
                    }
                    if (null != session) {
                        session.disconnect();
                    }
                    LOGGER.info("Closed channel and session successful...");
                }
                // format and send mail.
                Map<String, File> attachments = new HashMap<>(5);
                StringBuilder body = new StringBuilder(request.getBody());
                Map<String, List<RemoteFileAlarmRequest.RemoteFile>> excels = new HashMap<>(5);
                for (Map.Entry<RemoteFileAlarmRequest.RemoteFile, String> entry : localFiles.entrySet()) {
                    RemoteFileAlarmRequest.RemoteFile remoteFile = entry.getKey();
                    if (remoteFile.isAttachment() || !isAcceptFormat(entry.getValue(), conf)) {
                        if ("excel".equals(remoteFile.getFormat())) {
                            //抽取要转换为Excel的文件
                            if (!excels.containsKey(remoteFile.getExcelName())) {
                                List<RemoteFileAlarmRequest.RemoteFile> sheets = new ArrayList<>();
                                RemoteFileAlarmRequest.RemoteFile newFile = new RemoteFileAlarmRequest.RemoteFile();
                                newFile.setPath(entry.getValue());
                                newFile.setSheetName(remoteFile.getSheetName());
                                sheets.add(newFile);
                                excels.put(remoteFile.getExcelName(), sheets);
                            } else {
                                List<RemoteFileAlarmRequest.RemoteFile> sheets = excels.get(remoteFile.getExcelName());
                                RemoteFileAlarmRequest.RemoteFile newFile = new RemoteFileAlarmRequest.RemoteFile();
                                newFile.setPath(entry.getValue());
                                newFile.setSheetName(remoteFile.getSheetName());
                                sheets.add(newFile);
                            }
                        } else {
                            attachments.put(remoteFile.getRename(), new File(entry.getValue()));
                        }
                    } else {
                        // format
                        String table = readFileAndFormatHTMLTable(new File(entry.getValue()));
                        if (null == table || table.trim().isEmpty()) {
                            continue;
                        }
                        LOGGER.debug("Generate table html: " + table);
                        // append to body
                        body.append(NEW_LINE).append(table).append(NEW_LINE);
                    }
                }
                //Excel转换
                attachments.putAll(readFileAndFormatExcel(tmpDir.getAbsolutePath(), excels));
                AlarmResponse response = sendMailMessage(body.toString(), attachments);
                LOGGER.info("Sent mail message successful. Downloaded " + localFiles.size() + " remote files from " + request.getUser() + "@" + request.getHost() + ".");
                // clean downloaded files.
                FileUtils.deleteQuietly(tmpDir);
                return response;
            }
            default:
                throw new AlarmException("Not found remote type [" + request.getRemoteType() + "] execution.");
        }
    }

    private String readFileAndFormatHTMLTable(File file) {
        HtmlTableBuilder table = new HtmlTableBuilder(1, 0, 6);
        try (FileReader fr = new FileReader(file)) {
            BufferedReader reader = new BufferedReader(fr);
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                // dump data file default split is '\001', header split is '|'
                String[] cells = i == 0 ? line.split("\\|") : line.split("\\001");
                table.appendRow(cells, i == 0);
                i++;
            }
            // close read buffer.
            reader.close();
        } catch (IOException e) {
            LOGGER.error("Format file content to table exception.", e);
        }
        return table.build();
    }

    private Map<String, File> readFileAndFormatExcel(String tmpDirPath, Map<String, List<RemoteFileAlarmRequest.RemoteFile>> excels) {
        Map<String, File> result = new HashMap<>(excels.size());
        for (Map.Entry<String, List<RemoteFileAlarmRequest.RemoteFile>> excel : excels.entrySet()) {
            result.put(excel.getKey(), new ExcelBuilder().build(tmpDirPath, excel.getKey(), excel.getValue()));
        }
        return result;
    }

    private boolean isAcceptFormat(String file, Configuration conf) {
        String strAcceptFileExt = conf.get(ALARM_MAIL_BODY_FORMAT_FILE, null);
        if (null == strAcceptFileExt) {
            LOGGER.warn("There's not configure mail body format file ext with configure key: " + ALARM_MAIL_BODY_FORMAT_FILE);
            return false;
        }
        String fileExt = "." + FilenameUtils.getExtension(file);
        String[] acceptFileExts = strAcceptFileExt.split(",");
        for (String acceptFileExt : acceptFileExts) {
            if (acceptFileExt.equalsIgnoreCase(fileExt)) {
                return true;
            }
        }
        return false;
    }
}
