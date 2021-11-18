/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.jdbc.NebulaAbstractPreparedStatement;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NebulaPreparedStatement extends NebulaAbstractPreparedStatement {

    public NebulaPreparedStatement(NebulaConnection connection, String rawNGQL) {
        super(connection, rawNGQL);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        this.checkReadOnly(this.rawNGQL);
        this.execute();
        return currentResultSet;
    }

    @Override
    /** This method will not return the number of data which influenced by the executed ngql.
     *  If the user inserts multiple points or edges in a single insert statement, some of them may succeed,
     *  but the server will only return to tell the user that it has failed, however, the user may actually be able to find out some of the data have inserted.
     */
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

    public String replacePlaceHolderWithParam(String rawNGQL) throws SQLException {
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
    public void setDate(int parameterIndex, java.sql.Date date) throws SQLException {
        insertParameter(parameterIndex, date);
    }

    @Override
    public void setTime(int parameterIndex, java.sql.Time time) throws SQLException {
        insertParameter(parameterIndex, time);
    }

    public void setDatetime(int parameterIndex, java.util.Date datetime) throws SQLException {
        insertParameter(parameterIndex, datetime);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return NebulaParameterMetaData.getInstance(this);
    }


}
