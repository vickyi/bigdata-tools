package com.aiplus.bi.etl.input.druid;

import com.aiplus.bi.etl.input.DataInputJobConfiguration;
import com.aiplus.bi.etl.input.OutputHiveSchemaTransfer;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dev
 */
public class DruidOutputHiveSchemaTransfer extends OutputHiveSchemaTransfer {

    private static final String QUERY_SEGMENT_LIST = "SELECT payload " +
            "FROM druid_segments " +
            "WHERE `used` = 1 AND datasource = ? AND convert_tz(`start`, '+00:00', '+08:00') LIKE ? " +
            "ORDER BY UNIX_TIMESTAMP(`start`) DESC, id DESC " +
            "LIMIT 1";

    private String strExportDate;

    public DruidOutputHiveSchemaTransfer(DataInputJobConfiguration etlJobConfiguration, String etlJobId, String strExportDate) {
        super(etlJobConfiguration, etlJobId);
        this.strExportDate = strExportDate;
    }

    @Override
    protected List<String> doHiveSchemaOutput() throws IOException {
        List<String> hiveCreateSchemaDDLs = new ArrayList<>();
        String hiveSchema = getEtlJobConfiguration().getOutput().getHiveSchema();
        if ("base".equals(hiveSchema)) {
            hiveSchema = "ods";
        }
        String externalPath = getEtlJobConfiguration().getOutput().getHiveExternalPath();
        for (DataInputJobConfiguration.TableMapper tableMapper : getEtlJobConfiguration().getMappers()) {
            String ddl = generateDruidHiveSchema(tableMapper.getSourceTable(), tableMapper.getTargetTable(), hiveSchema, externalPath);
            if (null != ddl) {
                hiveCreateSchemaDDLs.add(ddl);
            }
        }
        return hiveCreateSchemaDDLs;
    }

    private String generateDruidHiveSchema(String dataSource, String targetTableName, String hiveSchema, String externalPath) throws IOException {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        GsonBuilder gsonBuilder = new GsonBuilder().serializeNulls();
        String dimensions = null;
        String metrics = null;
        try {
            conn = getSourceConnection();
            pstmt = conn.prepareStatement(QUERY_SEGMENT_LIST);
            pstmt.setString(1, dataSource);
            pstmt.setString(2, strExportDate + "%");
            rs = pstmt.executeQuery();
            if (rs.next()) {
                String strPayLoad = rs.getString("payload");
                JsonObject json = gsonBuilder.create().fromJson(strPayLoad, JsonObject.class);
                dimensions = json.get("dimensions").getAsString();
                metrics = json.get("metrics").getAsString();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            if (null != rs) {
                try {
                    rs.close();
                } catch (SQLException ignored) {
                }
            }
            if (null != pstmt) {
                try {
                    pstmt.close();
                } catch (SQLException ignored) {
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (SQLException ignored) {
                }
            }
        }
        if (null == dimensions || null == metrics) {
            return null;
        }
        String[] dimensionArray = dimensions.split(",");
        String[] metricArray = metrics.split(",");
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(hiveSchema).append(".`").append(targetTableName)
                .append("` (druid_timestamp bigint, druid_year int, druid_month int, druid_day int, druid_hour int, druid_minute int, data_utc_timestamp string, ");
        for (String aDimensionArray : dimensionArray) {
            ddl.append("`").append(aDimensionArray).append("` string, ");
        }
        for (int i = 0; i < metricArray.length; i++) {
            ddl.append("`").append(metricArray[i]).append("` bigint");
            if (i != metricArray.length - 1) {
                ddl.append(", ");
            }
        }
        ddl.append(") PARTITIONED BY(druid_date string);");
        // 先尝试把之前的数据清理掉
        ddl.append("ALTER TABLE ").append(hiveSchema).append(".`").append(targetTableName).append("` ")
                .append("DROP IF EXISTS PARTITION(druid_date=\"").append(strExportDate).append("\");");
        // 数据加载到分区
        ddl.append("LOAD DATA INPATH \"").append(externalPath).append("/").append(getEtlJobId()).append("/").append(dataSource).append("\" ")
                .append("INTO TABLE ").append(hiveSchema).append(".`").append(targetTableName).append("` ")
                .append("PARTITION(druid_date=\"").append(strExportDate).append("\");");
        ddl.trimToSize();
        return ddl.toString();
    }
}
