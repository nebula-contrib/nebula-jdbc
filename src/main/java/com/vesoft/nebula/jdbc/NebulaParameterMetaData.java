/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import com.vesoft.nebula.jdbc.statement.NebulaPreparedStatementImpl;
import com.vesoft.nebula.jdbc.statement.NebulaStatement;
import com.vesoft.nebula.jdbc.utils.ExceptionBuilder;
import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class NebulaParameterMetaData implements ParameterMetaData{

    private static NebulaParameterMetaData nebulaParameterMetaData = null;
    private NebulaPreparedStatementImpl preparedStatement;

    private NebulaParameterMetaData(NebulaPreparedStatementImpl preparedStatement){
        this.preparedStatement = preparedStatement;
    }

    public static ParameterMetaData getInstance(NebulaPreparedStatementImpl preparedStatement){
        if (nebulaParameterMetaData == null){
            nebulaParameterMetaData = new NebulaParameterMetaData(preparedStatement);
        }
        return nebulaParameterMetaData;
    }

    /**  If you want to get MetaData after PreparedStatement changes, the method below should be called. */
    public static void release(){
        nebulaParameterMetaData = null;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return preparedStatement.getParametersNumber();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        Object parameter =  preparedStatement.getParameters().get(param);
        if(parameter != null){
            return parameter.getClass().getName();
        }
        return String.format("No such param with index [%d]", param);
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getScale(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
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
