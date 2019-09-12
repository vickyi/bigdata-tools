package com.aiplus.bi.alarm.mail;

import com.aiplus.bi.alarm.core.RemoteFileAlarmRequest;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.List;

/**
 * 其他文件 -> Excel文件
 *
 * @author dev
 */
public class ExcelBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExcelBuilder.class);

    private static final String EXCEL_SUFFIX = ".xlsx";

    private SXSSFWorkbook swb;

    ExcelBuilder() {
        swb = new SXSSFWorkbook(1000);
    }

    public File build(String tmpDirPath, String fileName, List<RemoteFileAlarmRequest.RemoteFile> remoteFiles) {
        File newFile = new File(tmpDirPath + "/" + fileName.substring(0, fileName.lastIndexOf(".")) + EXCEL_SUFFIX);
        for (RemoteFileAlarmRequest.RemoteFile remoteFile : remoteFiles) {
            buildExcel(remoteFile);
        }
        writeExcel(newFile);
        close();
        return newFile;
    }

    private void buildExcel(RemoteFileAlarmRequest.RemoteFile remoteFile) {
        try (FileReader fr = new FileReader(new File(remoteFile.getPath()));
             BufferedReader reader = new BufferedReader(fr)) {
            String line;
            int i = 0;
            SXSSFSheet sheet = swb.createSheet(remoteFile.getSheetName());
            while ((line = reader.readLine()) != null) {
                SXSSFRow row = sheet.createRow(i);
                String[] cells = i == 0 ? line.split("\\|") : line.split("\\001");
                for (int j = 0; j < cells.length; j++) {
                    row.createCell(j).setCellValue(cells[j]);
                }
                i++;
            }
        } catch (Exception e) {
            LOGGER.error("build excel exception.", e);
        }
    }

    private void writeExcel(File file) {
        try (FileOutputStream out = new FileOutputStream(file)) {
            swb.write(out);
        } catch (Exception e) {
            LOGGER.error("write excel exception.", e);
        }
    }

    private void close() {
        if (swb != null) {
            try {
                swb.close();
            } catch (Exception e) {
                LOGGER.error("close SXSSFWorkbook exception.", e);
            }
        }
    }
}
