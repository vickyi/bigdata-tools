package com.aiplus.bi.alarm.core;

/**
 * 远程文件形式的告警请求.
 *
 * @author dev
 */
public class RemoteFileAlarmRequest extends AlarmRequest {

    /**
     * 远程类型：sftp（使用sftp下载文件到本地）, upload（上传文件到本地）
     */
    private String remoteType;
    /**
     * 远程文件集合.
     */
    private RemoteFile[] remoteFiles;

    public RemoteFileAlarmRequest() {
        setType("remote_file");
    }

    public String getRemoteType() {
        return remoteType;
    }

    public void setRemoteType(String remoteType) {
        this.remoteType = remoteType;
    }

    public RemoteFile[] getRemoteFiles() {
        return remoteFiles;
    }

    public void setRemoteFiles(RemoteFile[] remoteFiles) {
        this.remoteFiles = remoteFiles;
    }

    public static class RemoteFile {
        /**
         * 文件名（原始文件名）
         */
        private String name;
        /**
         * 重命名（文件下载到本地以后的名字，也是对外显示的名字，如果这个字段为空，则使用name）
         */
        private String rename;
        /**
         * 文件路径（在远程服务器上的绝对路径）
         */
        private String path;
        /**
         * 是否将文件作为附件处理，默认为True
         */
        private boolean attachment = true;

        /**
         * 内容格式化：TEXT， TABLE（只支持CSV格式）, excel
         */
        private String format = "text";
        /**
         * 要写入的Excel文件名(只在format为excel时有效)
         */
        private String excelName;
        /**
         * 要写入的sheet名(只在format为excel时有效)
         */
        private String sheetName;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getRename() {
            return rename;
        }

        public void setRename(String rename) {
            this.rename = rename;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public boolean isAttachment() {
            return attachment;
        }

        public void setAttachment(boolean attachment) {
            this.attachment = attachment;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String format) {
            this.format = format;
        }

        public String getExcelName() {
            return excelName;
        }

        public void setExcelName(String excelName) {
            this.excelName = excelName;
        }

        public String getSheetName() {
            return sheetName;
        }

        public void setSheetName(String sheetName) {
            this.sheetName = sheetName;
        }
    }
}
