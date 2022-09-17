/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.statement;

import com.vesoft.nebula.jdbc.NebulaConnection;
import com.vesoft.nebula.jdbc.NebulaResultSet;
import com.vesoft.nebula.jdbc.utils.ExceptionBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaStatementImpl implements NebulaStatement {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final String[] updateKeyword = {"update", "delete", "insert", "upsert", "create", "drop", "alter", "rebuild"};
    protected final String[] queryKeyword = {"match", "lookup", "go", "fetch", "find", "subgraph"};

    protected String nGql;
    protected NebulaConnection nebulaConnection;
    protected boolean isExecuteSucceed;
    protected ResultSet currentResultSet;
    protected boolean isClosed = false;


    public NebulaStatementImpl(NebulaConnection connection) {
        this.nebulaConnection = connection;
    }


    /** This method just return a boolean value to indicate whether succeed or not,
     *  if you call it directly you can call getResultSet() to get currentResultSet to retrieve result from serve.
     */
    @Override
    public boolean execute(String nGql) throws SQLException {
        this.checkClosed();
        this.nGql = nGql;
        com.vesoft.nebula.client.graph.data.ResultSet nebulaResultSet = this.nebulaConnection.execute(nGql);
        isExecuteSucceed = nebulaResultSet.isSucceeded();
        if(!isExecuteSucceed){
            int errorCode = nebulaResultSet.getErrorCode();
            String errorMessage = nebulaResultSet.getErrorMessage();
            throw new SQLException(String.format("nGql \"%s\" executed failed, error code: %d, error message: %s", nGql, errorCode, errorMessage));
        }
        this.currentResultSet = new NebulaResultSet(nebulaResultSet, this);
        return true;
    }

    @Override
    public ResultSet executeQuery(String nGql) throws SQLException {
        this.checkReadOnly(nGql);
        this.execute(nGql);
        return currentResultSet;
    }


    /** This method will not return the number of data which influenced by the executed ngql.
     *  If the user inserts multiple points or edges in a single insert statement, some of them may succeed,
     *  but the server will only return to tell the user that it has failed, however, the user may actually be able to find out some of the data have inserted.
    */
    @Override
    public int executeUpdate(String nGql) throws SQLException {
        this.checkUpdate(nGql);
        this.execute(nGql);
        return 0;
    }

    @Override
    public NebulaConnection getConnection() throws SQLException {
        return this.nebulaConnection;
    }



    @Override
    public void checkReadOnly(String nGql) throws SQLException {
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

    @Override
    public void checkUpdate(String nGql) throws SQLException {
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


    @Override
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
