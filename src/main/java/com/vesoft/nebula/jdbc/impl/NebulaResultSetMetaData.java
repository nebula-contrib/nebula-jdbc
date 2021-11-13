/**
 * @author Flash
 * @Email flash@m.scnu.edu.cn
 */

package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.jdbc.NebulaAbstractResultSetMetaData;

import java.sql.SQLException;

public class NebulaResultSetMetaData extends NebulaAbstractResultSetMetaData {

    private static NebulaResultSetMetaData nebulaResultSetMetaData = null;

    private NebulaResultSetMetaData(NebulaResultSet nebulaResultSet) {
        super(nebulaResultSet);
    }

    private NebulaResultSetMetaData(){
        super();
    }

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

}
