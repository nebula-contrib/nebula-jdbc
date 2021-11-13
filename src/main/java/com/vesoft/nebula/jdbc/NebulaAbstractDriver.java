/**
 * @author Flash
 * @Email flash@m.scnu.edu.cn
 */

package com.vesoft.nebula.jdbc;

import com.vesoft.nebula.jdbc.utils.ExceptionBuilder;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public abstract class NebulaAbstractDriver implements java.sql.Driver{

    /**
     * JDBC prefix for the connection url.
     * */
    protected static final String JDBC_PREFIX = "jdbc:nebula://";

    protected Properties poolProperties = new Properties();

    protected Properties connectionConfig = new Properties();

    protected NebulaAbstractDriver() {
    }

    @Override public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("Invalid url: null.");
        }
        String[] pieces = url.split("//");
        if (url.startsWith(JDBC_PREFIX) && pieces.length == 2) {
            return true;
        }
        return false;
    }

    protected void parseUrlProperties(String url, Properties connectionConfig) {
        Properties parseUrlGetConfig = new Properties();

        if(connectionConfig != null) {
            for (Map.Entry<Object, Object> entry : connectionConfig.entrySet()) {
                parseUrlGetConfig.put(entry.getKey().toString().toLowerCase(), entry.getValue());
            }
        }

        String graphSpace = url.split("//")[1];
        parseUrlGetConfig.put("graphSpace", graphSpace);
        parseUrlGetConfig.put("url", url);
        this.connectionConfig = parseUrlGetConfig;
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
     * -----------------------Not implement yet-------------------------
     *
     * */

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

}
