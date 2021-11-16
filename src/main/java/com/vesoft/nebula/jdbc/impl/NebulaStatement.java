/**
 * @author Flash
 * @Email flash@m.scnu.edu.cn
 */

package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.jdbc.NebulaAbstractStatement;

import java.sql.ResultSet;
import java.sql.SQLException;

public class NebulaStatement extends NebulaAbstractStatement {

    public NebulaStatement(NebulaConnection connection) {
        super(connection);
    }

    @Override
    /** This method just return a boolean value to indicate whether succeed or not,
     *  if you call it directly you can call getResultSet() to get currentResultSet to retrieve result from serve.
     */
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

    @Override
    /** This method will not return the number of data which influenced by the executed ngql.
     *  If the user inserts multiple points or edges in a single insert statement, some of them may succeed,
     *  but the server will only return to tell the user that it has failed, however, the user may actually be able to find out some of the data have inserted.
    */
    public int executeUpdate(String nGql) throws SQLException {
        this.checkUpdate(nGql);
        this.execute(nGql);
        return 0;
    }

    @Override
    public NebulaConnection getConnection() throws SQLException {
        return this.nebulaConnection;
    }

}
