package com.aiplus.bi.etl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author dev
 */
public class MetadataConfiguration implements MetadataConfigurable {

    private static final Log LOG = LogFactory.getLog(MetadataConfiguration.class);

    private static final String METADATA_ETC_FILE_NAME = "metadata-etc";

    private static final String SQL_QUERY_DATA_SOURCE_INFO = "SELECT ID, SOURCE_CODE, SOURCE_URL, SOURCE_USER, SOURCE_PASSWORD, SOURCE_TYPE FROM BI_DATASOURCE WHERE SOURCE_CODE = ?";

    private Map<String, Object> configs = new HashMap<>();

    public MetadataConfiguration() {
        loadPropertiesFromClasspath();
    }

    public static Connection getJdbcConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public String getProperty(String key) {
        return configs.containsKey(key) ? configs.get(key) + "" : null;
    }

    public int getInt(String key, int def) {
        return configs.containsKey(key) ? Integer.parseInt(getProperty(key)) : def;
    }

    public boolean getBoolean(String key, boolean def) {
        return configs.containsKey(key) ? Boolean.parseBoolean(getProperty(key)) : def;
    }

    public Connection getMetadataConnection() throws SQLException {
        return MetadataConfiguration.getJdbcConnection(getProperty(METADATA_JDBC_URL_KEY), getProperty(METADATA_JDBC_USER_KEY), getProperty(METADATA_JDBC_PWD_KEY));
    }

    public DataSourceConnectionInfo getDataSourceConnectionInfo(String dataSourceCode) throws SQLException {
        try (Connection conn = getMetadataConnection(); PreparedStatement pstmt = conn.prepareStatement(SQL_QUERY_DATA_SOURCE_INFO)) {
            pstmt.setString(1, dataSourceCode);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new DataSourceConnectionInfo(rs.getString("SOURCE_URL"), rs.getString("SOURCE_USER"), rs.getString("SOURCE_PASSWORD"));
                } else {
                    throw new NullPointerException("DataSource[" + dataSourceCode + "] not found on config database.");
                }
            }
        }
    }

    public Connection getDatasourceConnection(DataSourceConnectionInfo datasourceConnectionInfo) throws SQLException {
        return MetadataConfiguration.getJdbcConnection(datasourceConnectionInfo.getUrl(), datasourceConnectionInfo.getUser(), datasourceConnectionInfo.getPassword());
    }

    public Connection getDatasourceConnection(String dataSourceCode) throws SQLException {
        DataSourceConnectionInfo datasourceConnectionInfo = getDataSourceConnectionInfo(dataSourceCode);
        return getDatasourceConnection(datasourceConnectionInfo);
    }

    @Override
    public MetadataConfiguration getMetadataConfiguration() {
        return this;
    }

    @Override
    public void setMetadataConfiguration(MetadataConfiguration configuration) {
        this.configs.putAll(configuration.configs);
    }

    private void loadPropertiesFromClasspath() {
        // get env
        String profile = System.getProperty(METADATA_PROFILE_KEY);
        String configFileName = METADATA_ETC_FILE_NAME;
        if (null != profile && !"".equals(profile)) {
            configFileName = configFileName + "-" + profile;
        }
        configFileName = configFileName + ".properties";
        LOG.info("Load configuration from: " + configFileName);

        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFileName);
        if (null != in) {
            Properties props = new Properties();
            try {
                props.load(in);
            } catch (IOException e) {
                throw new RuntimeException("Properties not load from file: " + configFileName, e);
            }
            // 转到Map里面去
            Enumeration<?> eum = props.propertyNames();
            while (eum.hasMoreElements()) {
                Object key = eum.nextElement();
                Object obj = props.get(key);
                if (null != obj) {
                    LOG.info("Load property: " + key + " -> " + (key.toString().contains("password") ? "********" : obj));
                    configs.put(String.valueOf(key), obj);
                }
            }
        }
    }

    public static class DataSourceConnectionInfo {

        private String url;
        private String user;
        private String password;

        public DataSourceConnectionInfo(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}
