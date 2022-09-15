/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import com.vesoft.nebula.jdbc.utils.ExceptionBuilder;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class NebulaResultSetMetaData implements ResultSetMetaData {

    private NebulaResultSet nebulaResultSet;
    private static NebulaResultSetMetaData nebulaResultSetMetaData = null;

    private NebulaResultSetMetaData(NebulaResultSet nebulaResultSet) {
        this.nebulaResultSet = nebulaResultSet;
    }

    private NebulaResultSetMetaData(){ }

    public static NebulaResultSetMetaData getInstance(NebulaResultSet nebulaResultSet){
        if(nebulaResultSetMetaData == null){
            nebulaResultSetMetaData = new NebulaResultSetMetaData(nebulaResultSet);
        }
        return nebulaResultSetMetaData;
    }

    /**  If you want to get MetaData after ResultSet changes, the method below should be called. */
    public static void release(){
        nebulaResultSetMetaData = null;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.nebulaResultSet.getColumnNames().size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        int columnCount = this.getColumnCount();
        if(column <= columnCount && column > 0){
            return this.nebulaResultSet.getColumnNames().get(column - 1);
        }else {
            throw new SQLException(String.format("The numbers of column is [%d], your column index [%d] is invalid.", columnCount, column));
        }
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        int columnCount = this.getColumnCount();
        if(column <= columnCount && column > 0){
            return this.nebulaResultSet.getNativeNebulaResultSet().getSpaceName();
        }else {
            throw new SQLException(String.format("The numbers of column is [%d], your column index [%d] is invalid.", columnCount, column));
        }
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getScale(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
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
