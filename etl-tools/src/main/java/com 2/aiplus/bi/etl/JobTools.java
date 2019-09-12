package com.aiplus.bi.etl;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

/**
 * Job的工具类.
 *
 * @author dev
 */
public class JobTools {

    private static final Log LOG = LogFactory.getLog(JobTools.class);

    /**
     * 执行命令.
     *
     * @param jobId    Job id.
     * @param commands 命令集合
     */
    public static void runCommand(String jobId, String[] commands) {
        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);
        try {
            Process process = pb.start();
            try (InputStream in = process.getInputStream(); InputStreamReader isr = new InputStreamReader(in)) {
                BufferedReader reader = new BufferedReader(isr);
                String line;
                while ((line = reader.readLine()) != null) {
                    LOG.info("[" + jobId + "]\t" + line);
                }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);

        }
    }

    /**
     * 提交一个Job，触发运行.
     *
     * @param job     要运行的Job
     * @param jobArgs Job的运行参数
     * @return 运行结果
     */
    public static int submitJob(ConfigurableJob job, String[] jobArgs) {
        MetadataConfiguration configuration = new MetadataConfiguration();
        job.setMetadataConfiguration(configuration);
        job.prepared(jobArgs);
        return job.signal();
    }

    /**
     * 解压一个Zip文件到指定文件夹里面.
     *
     * @param zipFile  Zip文件
     * @param unZipDir 解压后的文件夹
     */
    public static void unZip(File zipFile, File unZipDir) {
        if (!zipFile.exists()) {
            throw new NullPointerException("Zip file[" + zipFile.getAbsolutePath() + "] not exists.");
        }
        try (ZipArchiveInputStream zais = new ZipArchiveInputStream(new BufferedInputStream(new FileInputStream(zipFile), 1024))) {
            // Create un zip dir
            FileUtils.forceMkdir(unZipDir);
            ZipArchiveEntry entry;
            while ((entry = zais.getNextZipEntry()) != null) {
                LOG.debug("UnZip file: " + entry.getName());
                if (entry.isDirectory()) {
                    FileUtils.forceMkdir(new File(unZipDir, entry.getName()));
                } else {
                    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(new File(unZipDir, entry.getName())), 1024)) {
                        IOUtils.copy(zais, os);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("ZipError: " + zipFile.getAbsolutePath(), e);
        }
    }
}
