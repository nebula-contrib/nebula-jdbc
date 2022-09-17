/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.statement;

import com.vesoft.nebula.jdbc.NebulaConnection;
import com.vesoft.nebula.jdbc.NebulaParameterMetaData;

import com.vesoft.nebula.jdbc.utils.ExceptionBuilder;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NebulaPreparedStatementImpl extends NebulaStatementImpl implements NebulaPreparedStatement {
    protected String                  rawNGQL;
    protected String                  nGql;
    protected HashMap<Object, Object> parameters;
    protected int                     parametersNumber;

    public NebulaPreparedStatementImpl(NebulaConnection connection, String rawNGQL) {
        super(connection);
       this.rawNGQL = rawNGQL;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        this.checkReadOnly(this.rawNGQL);
        this.execute();
        return currentResultSet;
    }


    /** This method will not return the number of data which influenced by the executed ngql.
     *  If the user inserts multiple points or edges in a single insert statement, some of them may succeed,
     *  but the server will only return to tell the user that it has failed, however, the user may actually be able to find out some of the data have inserted.
     */
    @Override
    public int executeUpdate() throws SQLException {
        this.checkUpdate(this.rawNGQL);
        this.execute();
        return 0;
    }

    @Override
    public boolean execute() throws SQLException {
        this.nGql = replacePlaceHolderWithParam(this.rawNGQL);
        return this.execute(this.nGql);
    }

    private String replacePlaceHolderWithParam(String rawNGQL) throws SQLException {
        Integer index = 1;
        String digested = rawNGQL;

        String regex = "\\?(?=[^\"]*(?:\"[^\"]*\"[^\"]*)*$)";
        Matcher matcher = Pattern.compile(regex).matcher(digested);

        while (matcher.find()) {
            Object param = parameters.get(index);
            if(param == null){
                throw new SQLException(String.format("Can not get param in index [%d], please check your nGql.", index));
            }

            String paramTypeName = param.getClass().getTypeName();
            switch (paramTypeName){
                case ("java.lang.String"):
                    param = String.format("\"%s\"", param);
                    break;
                case ("java.sql.Date"):
                    param = String.format("date(\"%s\")", param);
                    break;
                case ("java.sql.Time"):
                    param = String.format("time(\"%s\")", param);
                    break;
                case ("java.util.Date"):
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss");
                    String datetimeString = formatter.format(param);
                    param = String.format("datetime(\"%s\")", datetimeString);
                    break;
                default:
                    break;
            }

            digested = digested.replaceFirst(regex, param.toString());
            index++;
        }

        return digested;
    }

    /**  set methods  */

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        insertParameter(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date date) throws SQLException {
        insertParameter(parameterIndex, date);
    }

    @Override
    public void setTime(int parameterIndex, Time time) throws SQLException {
        insertParameter(parameterIndex, time);
    }

    public void setDatetime(int parameterIndex, java.util.Date datetime) throws SQLException {
        insertParameter(parameterIndex, datetime);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return NebulaParameterMetaData.getInstance(this);
    }


    @Override
    public void checkParamsNumber(int parameterIndex) throws SQLException {
        if (parameterIndex > this.parametersNumber) {
            throw new SQLException("ParameterIndex does not correspond to a parameter marker in the nGQL statement.");
        }
    }

    protected int namedParameterCount(String rawNGQL) {
        int max = 0;
        String regex = "\\?(?=[^\"]*(?:\"[^\"]*\"[^\"]*)*$)";
        Matcher matcher = Pattern.compile(regex).matcher(rawNGQL);
        while (matcher.find()) {
            max++;
        }
        return max;
    }

    @Override
    public void insertParameter(int parameterIndex, Object obj) throws SQLException {
        this.checkClosed();
        this.checkParamsNumber(parameterIndex);
        this.parameters.put(parameterIndex, obj);
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return currentResultSet != null ? currentResultSet.getMetaData() : null;
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        this.parameters.clear();
    }

    public HashMap<Object, Object> getParameters() {
        return parameters;
    }

    public int getParametersNumber() {
        return parametersNumber;
    }

    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Method execute(String, int) cannot be called on PreparedStatement.");
    }

    @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Method execute(String, int[]) cannot be called on PreparedStatement.");
    }

    @Override public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Method execute(String, String[]) cannot be called on PreparedStatement.");
    }

    @Override public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Method executeQuery(String) cannot be called on PreparedStatement.");
    }

    @Override public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Method executeUpdate(String) cannot be called on PreparedStatement.");
    }

    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Method executeUpdate(String, int) cannot be called on PreparedStatement.");
    }

    @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Method executeUpdate(String, int[]) cannot be called on PreparedStatement.");
    }

    @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Method executeUpdate(String, String[]) cannot be called on PreparedStatement.");
    }

    /**
     * -----------------------Not implement yet-------------------------
     *
     * */

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void addBatch() throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        throw  ExceptionBuilder.buildUnsupportedOperationException();
    }

}
