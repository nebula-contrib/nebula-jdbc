/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.jdbc.NebulaAbstractResultSetMetaData;
import com.vesoft.nebula.client.graph.data.ResultSet;

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
    
    public List<String> getColumnNames() {
        return nebulaResultSet == null ? null : nebulaResultSet.getColumnNames();
    }

    @Override
    public int getColumnCount() throws SQLException {
        List<String> names = getColumnNames();
        return names == null ? 0 : names.size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        List<String> names = getColumnNames();
        if(names != null && column <= names.size() && column > 0){
            return names.get(column - 1);
        }
        throw new SQLException(String.format("The numbers of column is [%d], your column index [%d] is invalid.", columnCount, column));
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        ResultSet rs = nebulaResultSet.getNativeNebulaResultSet();
        return rs == null ? null : rs.getSpaceName();
    }

}
