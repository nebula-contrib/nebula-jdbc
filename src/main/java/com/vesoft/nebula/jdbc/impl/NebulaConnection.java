/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.jdbc.NebulaAbstractConnection;
import com.vesoft.nebula.jdbc.NebulaAbstractResultSet;

import com.vesoft.nebula.jdbc.utils.ExceptionBuilder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaConnection implements Connection {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private NebulaDriver nebulaDriver;

    private Session nebulaSession;
    private String graphSpace = null;
    private boolean isClosed = false;
    private Properties connectionConfig;
    private int holdability;


    protected NebulaConnection(NebulaDriver nebulaDriver, String graphSpace) throws SQLException {
        this.holdability = NebulaAbstractResultSet.CLOSE_CURSORS_AT_COMMIT;
        this.nebulaDriver = nebulaDriver;
        this.nebulaSession = this.nebulaDriver.getSessionFromNebulaPool();
        this.connectionConfig = this.nebulaDriver.getConnectionConfig();

        // check whether access the given graph space successfully.
        try {
            ResultSet result = nebulaSession.execute("use " + graphSpace);
            if (result.isSucceeded()) {
                this.graphSpace = graphSpace;
                log.info(String.format("Access graph space [%s] succeeded", graphSpace));
            } else {
                throw new SQLException(String.format("Access graph space [%s] failed. Error code:" +
                                " %d, Error message: %s",
                        graphSpace, result.getErrorCode(), result.getErrorMessage()));
            }
        } catch (IOErrorException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet execute(String nGql) throws SQLException {
        this.checkClosed();
        try {
            return nebulaSession.execute(nGql);
        } catch (IOErrorException e) {
            throw new SQLException(e.getMessage());
        }
    }

    private void checkClosed() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Connection already closed.");
        }
    }

    @Override
    public void close() throws SQLException {
        this.checkClosed();
        this.nebulaSession.release();
        this.isClosed = true;
        log.info("JDBCConnection closed");

    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }


    @Override
    public Statement createStatement() throws SQLException {
        this.checkClosed();
        return new NebulaStatement(this);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        log.info("You are calling method \"Statement createStatement(int resultSetType, int " +
                "resultSetConcurrency)\", " +
                "the supported type is [TYPE_SCROLL_INSENSITIVE] and the supported concurrency is" +
                " [CONCUR_READ_ONLY]. " +
                "That is, the method you call is the same as \"Statement createStatement()\". ");
        return this.createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        log.info("You are calling method \"Statement createStatement(int resultSetType, int " +
                "resultSetConcurrency, int resultSetHoldability)\", " +
                "the supported type is [TYPE_SCROLL_INSENSITIVE], " +
                "the supported concurrency is [CONCUR_READ_ONLY]" +
                "and the supported holdability is [CLOSE_CURSORS_AT_COMMIT]." +
                "That is, the method you call is the same as \"Statement createStatement()\". ");
        return this.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String rawNGQL) throws SQLException {
        this.checkClosed();
        return new NebulaPreparedStatement(this, rawNGQL);
    }

    @Override
    public PreparedStatement prepareStatement(String nGql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        log.info("You are calling method \"PreparedStatement prepareStatement(String nGql, int " +
                "resultSetType, int resultSetConcurrency)\", " +
                "the supported type is [TYPE_SCROLL_INSENSITIVE] and the supported concurrency is" +
                " [CONCUR_READ_ONLY]. " +
                "That is, the method you call is the same as \"PreparedStatement prepareStatement" +
                "(String nGql)\". ");
        return this.prepareStatement(nGql);
    }

    @Override
    public PreparedStatement prepareStatement(String nGql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        log.info("You are calling method \"PreparedStatement prepareStatement(String nGql, int " +
                "resultSetType, int resultSetConcurrency, int resultSetHoldability)\", " +
                "the supported type is [TYPE_SCROLL_INSENSITIVE] and the supported concurrency is" +
                " [CONCUR_READ_ONLY] " +
                "and the supported holdability is [CLOSE_CURSORS_AT_COMMIT]. " +
                "That is, the method you call is the same as \"PreparedStatement prepareStatement" +
                "(String nGql)\". ");
        return this.prepareStatement(nGql);
    }

    @Override
    public String getSchema() throws SQLException {
        return this.graphSpace;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return this.nebulaDriver.getPoolProperties();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return (String) (getClientInfo().getOrDefault(name, "null"));
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        // check whether change graph space successfully.
        try {
            ResultSet result = nebulaSession.execute("use " + schema);
            if (result.isSucceeded()) {
                this.graphSpace = schema;
                this.connectionConfig.setProperty("graphSpace", graphSpace);
                this.connectionConfig.setProperty("url", "jdbc:nebula://" + graphSpace);
                log.info(String.format("Change graph space to [%s] succeeded", graphSpace));
            } else {
                log.error(String.format("Change graph space to [%s] failed. Error code: %d, Error" +
                                " message: %s",
                        schema, result.getErrorCode(), result.getErrorMessage()));
            }
        } catch (IOErrorException e) {
            throw new SQLException(e);
        }

    }

    public Properties getConnectionConfig() {
        return this.connectionConfig;
    }


    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void rollback() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getCatalog() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
    }

    @Override
    public int getHoldability() throws SQLException {
        // CLOSE_CURSORS_AT_COMMIT
        return this.holdability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Timeout value mustn't be less 0");
        }

        if (isClosed()) {
            return false;
        }

        Statement statement = null;
        try {
            statement = createStatement();
            if (statement.execute("YIELD 1")) {
                return true;
            } else {
                return false;
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }
}
