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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class NebulaConnection extends NebulaAbstractConnection {

    private NebulaDriver nebulaDriver;

    private Session nebulaSession;
    private String graphSpace = "unknown";
    private boolean isClosed = false;

    protected NebulaConnection(NebulaDriver nebulaDriver, String graphSpace) throws SQLException {
        super(NebulaAbstractResultSet.CLOSE_CURSORS_AT_COMMIT);
        this.nebulaDriver = nebulaDriver;
        this.nebulaSession = this.nebulaDriver.getSessionFromNebulaPool();
        this.connectionConfig = this.nebulaDriver.getConnectionConfig();

        // check whether access the given graph space successfully.
        try{
            ResultSet result = nebulaSession.execute("use " + graphSpace);
            if(result.isSucceeded()){
                this.graphSpace = graphSpace;
                log.info(String.format("Access graph space [%s] succeeded", graphSpace));
            }else {
                throw new SQLException(String.format("Access graph space [%s] failed. Error code: %d, Error message: %s",
                        graphSpace, result.getErrorCode(), result.getErrorMessage()));
            }
        }catch (IOErrorException e){
            throw new SQLException(e);
        }
    }

    public ResultSet execute(String nGql) throws SQLException {
        this.checkClosed();
        try {
            return nebulaSession.execute(nGql) ;
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
        log.info("You are calling method \"Statement createStatement(int resultSetType, int resultSetConcurrency)\", " +
                "the supported type is [TYPE_SCROLL_INSENSITIVE] and the supported concurrency is [CONCUR_READ_ONLY]. " +
                "That is, the method you call is the same as \"Statement createStatement()\". ");
        return this.createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        log.info("You are calling method \"Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)\", " +
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
    public PreparedStatement prepareStatement(String nGql, int resultSetType, int resultSetConcurrency) throws SQLException {
        log.info("You are calling method \"PreparedStatement prepareStatement(String nGql, int resultSetType, int resultSetConcurrency)\", " +
                "the supported type is [TYPE_SCROLL_INSENSITIVE] and the supported concurrency is [CONCUR_READ_ONLY]. " +
                "That is, the method you call is the same as \"PreparedStatement prepareStatement(String nGql)\". ");
        return this.prepareStatement(nGql);
    }

    @Override
    public PreparedStatement prepareStatement(String nGql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        log.info("You are calling method \"PreparedStatement prepareStatement(String nGql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)\", " +
                "the supported type is [TYPE_SCROLL_INSENSITIVE] and the supported concurrency is [CONCUR_READ_ONLY] " +
                "and the supported holdability is [CLOSE_CURSORS_AT_COMMIT]. " +
                "That is, the method you call is the same as \"PreparedStatement prepareStatement(String nGql)\". ");
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
        try{
            ResultSet result = nebulaSession.execute("use " + schema);
            if(result.isSucceeded()){
                this.graphSpace = schema;
                this.connectionConfig.setProperty("graphSpace", graphSpace);
                this.connectionConfig.setProperty("url", "jdbc:nebula://" + graphSpace);
                log.info(String.format("Change graph space to [%s] succeeded", graphSpace));
            }else {
                log.error(String.format("Change graph space to [%s] failed. Error code: %d, Error message: %s",
                        schema, result.getErrorCode(), result.getErrorMessage()));
            }
        }catch (IOErrorException e){
            throw new SQLException(e);
        }

    }

    public Properties getConnectionConfig(){
        return this.connectionConfig;
    }
}
