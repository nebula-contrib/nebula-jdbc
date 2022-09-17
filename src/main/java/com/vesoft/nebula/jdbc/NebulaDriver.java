/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import static com.vesoft.nebula.jdbc.utils.NebulaJdbcUrlParser.JDBC_NEBULA_PREFIX;
import com.vesoft.nebula.jdbc.utils.ExceptionBuilder;
import com.vesoft.nebula.jdbc.utils.NebulaJdbcUrlParser;
import java.net.URISyntaxException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class NebulaDriver implements java.sql.Driver {

    private static final Logger log = LoggerFactory.getLogger(NebulaDriver.class);

    static {
        try {
            NebulaDriver driver = new NebulaDriver();
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        log.info("Driver registered");
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("Invalid url: null.");
        }
        String[] pieces = url.split("//");
        return url.startsWith(JDBC_NEBULA_PREFIX) && pieces.length == 2;
    }

    protected Properties parseUrlProperties(String url, Properties connectionConfig) throws URISyntaxException {
        return NebulaJdbcUrlParser.parse(url, connectionConfig);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * connect to the NebulaGraph
     */
    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("url: " + url + " is not accepted, " +
                    "url example: jdbc:nebula://host_ip1:port,host_ip2:port/graphSpace " +
                    "make sure your url match this format.");

        }
        NebulaConnection JdbcConnection = new NebulaConnection(url, properties);
        log.info("Get JDBCConnection succeeded");
        return JdbcConnection;

    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties copy = new Properties(info);
        Properties properties;
        try {
            properties = parseUrlProperties(url, info);
        } catch (URISyntaxException e) {
            properties = copy;
            log.error("counld not parse url {}", url, e);
        }
        return createDriverPropertyInfo(properties).toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }


    private List<DriverPropertyInfo> createDriverPropertyInfo(Properties info) {
        List<DriverPropertyInfo> driverPropertyInfos = new ArrayList<>();

        for (Map.Entry property : info.entrySet()) {
            DriverPropertyInfo propertyInfo = new DriverPropertyInfo(property.getKey().toString()
                    , property.getValue().toString());
            propertyInfo.required = false;
            driverPropertyInfos.add(propertyInfo);
        }

        return driverPropertyInfos;
    }
}
