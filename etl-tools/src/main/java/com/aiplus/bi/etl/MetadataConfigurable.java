package com.aiplus.bi.etl;

/**
 * 元数据的可配置化接口.
 *
 * @author dev
 */
public interface MetadataConfigurable {

    /**
     * 运行的环境.
     */
    String METADATA_PROFILE_KEY = "metadata.profile";

    /**
     * 元数据存储的数据库连接地址.
     */
    String METADATA_JDBC_URL_KEY = "metadata.jdbc.url";

    /**
     * 元数据存储的数据库连接用户.
     */
    String METADATA_JDBC_USER_KEY = "metadata.jdbc.user";

    /**
     * 元数据存储的数据库连接密码.
     */
    String METADATA_JDBC_PWD_KEY = "metadata.jdbc.password";

    /**
     * 获取元数据的配置对象.
     *
     * @return 配置对象
     */
    MetadataConfiguration getMetadataConfiguration();

    /**
     * 设置元数据配置对象.
     *
     * @param configuration 配置对象.
     */
    void setMetadataConfiguration(MetadataConfiguration configuration);
}
