/**
 * @author Flash
 * @Email flash@m.scnu.edu.cn
 */

package com.vesoft.nebula.jdbc;

import com.vesoft.nebula.jdbc.impl.NebulaConnection;
import com.vesoft.nebula.jdbc.utils.ExceptionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Arrays;
import java.util.List;

public abstract class NebulaAbstractStatement implements java.sql.Statement{

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final String[] updateKeyword = {"update", "delete", "insert", "upsert", "create", "drop", "alter", "rebuild"};
    protected final String[] queryKeyword = {"match", "lookup", "go", "fetch", "find", "subgraph"};

    protected String nGql;
    protected NebulaConnection nebulaConnection;
    protected boolean isExecuteSucceed;
    protected ResultSet currentResultSet;
    protected boolean isClosed = false;

    public NebulaAbstractStatement(NebulaConnection connection) {
        this.nebulaConnection = connection;
    }

    protected void checkReadOnly(String nGql) throws SQLException {
        String lowerCaseNGQL = nGql.toLowerCase();
        List<String> splitNGQL = Arrays.asList(lowerCaseNGQL.split(" "));
        for (String updateItem : updateKeyword) {
            if(splitNGQL.contains(updateItem)){
                throw new SQLException(String.format("Method executeQuery() can only execute nGql to query data, " +
                        "but the current nGql contains the update keyword [%s], " +
                        "please modify your nGql or use executeUpdate().", updateItem));
            }
        }
        for (String queryItem : queryKeyword) {
            if(splitNGQL.contains(queryItem)){
                return;
            }
        }
        throw new SQLException("Method executeQuery() can only execute nGql to query data, " +
                "but the current nGql do not contains any query keyword in " + Arrays.toString(queryKeyword) +
                ", please modify your nGql or use executeUpdate(), execute().");
    }

    protected void checkUpdate(String nGql) throws SQLException {
        String lowerCaseNGQL = nGql.toLowerCase();
        List<String> splitNGQL = Arrays.asList(lowerCaseNGQL.split(" "));
        for (String updateItem : updateKeyword) {
            if(splitNGQL.contains(updateItem)){
                return;
            }
        }
        throw new SQLException("Method executeUpdate() can only execute nGql to update data, " +
                "but the current nGql do not contains any update keyword in " + Arrays.toString(updateKeyword) +
                ", please modify your nGql or use executeQuery(), execute().");
    }

    @Override
    public void close() throws SQLException {
        this.isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    public void checkClosed() throws SQLException {
        if(this.isClosed){
            throw new SQLException("Statement already closed.");
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if(this.currentResultSet != null){
            log.info("Method getResultSet() should be called only once per result, " +
                    "after it was called, the currentResultSet will be set to null.");
            ResultSet returnedResultSet =  this.currentResultSet;
            this.currentResultSet = null;
            return returnedResultSet;
        }else {
            throw new SQLException("currentResultSet has been set to null, may have been called before.");
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return this.currentResultSet.getHoldability();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return this.currentResultSet.getConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return this.currentResultSet.getType();
    }

    /**
    * -----------------------Not implement yet-------------------------
     *
    * */

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
//        return ((NebulaResultSet)currentResultSet).getNativeNebulaResultSet().getLatency();
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void cancel() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }


    @Override
    public int getUpdateCount() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
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
