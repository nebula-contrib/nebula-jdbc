/**
 * @author Flash
 * @Email flash@m.scnu.edu.cn
 */

package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.*;
import com.vesoft.nebula.client.graph.exception.InvalidValueException;
import com.vesoft.nebula.jdbc.NebulaAbstractResultSet;

import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.*;
import java.util.*;

public class NebulaResultSet extends NebulaAbstractResultSet {

    private int resultRowSize;
    private List<String> columnNames;
    private int currentRowNumber = -1;
    private ResultSet.Record currentRow;
    private Statement statement;

    public NebulaResultSet(ResultSet nebulaResultSet, Statement statement) {
        super(nebulaResultSet);
        this.statement = statement;
        if(!nativeNebulaResultSet.isEmpty()){
            this.resultRowSize = nativeNebulaResultSet.rowsSize();
            this.columnNames = nativeNebulaResultSet.getColumnNames();
        }
    }

    public String getErrorMessage(){
        return this.getNativeNebulaResultSet().getErrorMessage();
    }

    @Override
    public void close() throws SQLException {
        this.isClosed = true;
    }

    public void checkClosed() throws SQLException {
        if(this.isClosed){
            throw new SQLException("ResultSet already closed.");
        }

    }

    /** Some methods to move the cursor in ResultSet */
    @Override
    public boolean next() throws SQLException {
        this.checkClosed();
        ++currentRowNumber;
        if(currentRowNumber < resultRowSize){
            return true;
        } else {
            currentRow = null;
            return false;
        }
    }

    @Override
    public boolean previous() throws SQLException {
        this.checkClosed();
        --currentRowNumber;
        if(currentRowNumber >= 0){
            return true;
        } else {
            currentRow = null;
            return false;
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentRowNumber == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return currentRowNumber == resultRowSize;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return currentRowNumber == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return currentRowNumber == resultRowSize - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        this.checkClosed();
        currentRowNumber = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        this.checkClosed();
        currentRowNumber = resultRowSize;
    }

    @Override
    public boolean first() throws SQLException {
        this.checkClosed();
        if (resultRowSize == 0){
            return false;
        }
        currentRowNumber = 0;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        this.checkClosed();
        if (resultRowSize == 0){
            return false;
        }
        currentRowNumber = resultRowSize - 1;
        return true;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        this.checkClosed();
        if(row > 0){
            if(row > resultRowSize){
                currentRowNumber = resultRowSize - 1;
            }else{
                currentRowNumber = row - 1;
            }
        }else if(row < 0){
            currentRowNumber = resultRowSize + row;
            if (Math.abs(currentRowNumber) > resultRowSize) {
                currentRowNumber = 0;
            }
        }else  {
            currentRowNumber = -1;
        }
        return true;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        this.checkClosed();
        if(rows > 0){
            currentRowNumber = Math.min(currentRowNumber + rows, resultRowSize - 1);
        }else {
            currentRowNumber = Math.max(currentRowNumber + rows, 0);
        }
        return false;
    }



    @Override
    public int getRow() throws SQLException {
        return currentRowNumber + 1;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        int index = 0;
        for(int i = 0; i < columnNames.size(); ++i){
            if(columnLabel.equals(columnNames.get(i))){
                index = i + 1;
            }
        }
        if(index <= 0){
            throw new SQLException(String.format("No such column [%s] found, please check your parameters.", columnLabel));
        }
        return index;
    }

    public void checkIndex(int index) throws SQLException {
        if(index <= 0 || index > columnNames.size()){
            throw new SQLException(String.format("column index [%d] is invalid, please check your parameters (the first one should be represent as 1 instead of 0).", index));
        }
    }

    public void checkNullValue() throws SQLException {
        if(this.wasNull()){
            throw new SQLException("The current value you accessing is __NULL__, " +
                    "please check your nGql");
        }
    }

    public void checkResultSetCursor() throws SQLException {
        if(currentRowNumber > resultRowSize || currentRowNumber < 0){
            throw new SQLException(String.format("The cursor of ResultSet is in invalid position [%d] (count from 0), please check it.", currentRowNumber));
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return this.lastColumnAccess.isNull();
    }

    /** some getter methods */

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        this.checkClosed();
        checkResultSetCursor();
        checkIndex(columnIndex);
        currentRow = nativeNebulaResultSet.rowValues(currentRowNumber);
        ValueWrapper result = currentRow.get(columnIndex - 1);
        this.lastColumnAccess = result;
        return result;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        try {
            ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
            this.checkNullValue();
            return valueWrapper.asString();
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("UnsupportedEncodingException occur in NebulaResult.getString().", e);
        }
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }


    @Override
    public int getInt(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
        this.checkNullValue();
        return new Long(valueWrapper.asLong()).intValue();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = ((ValueWrapper) getObject(columnIndex));
        this.checkNullValue();
        return valueWrapper.asLong();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
        this.checkNullValue();
        return valueWrapper.asBoolean();
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
        this.checkNullValue();
        return valueWrapper.getValue().getFVal();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
        this.checkNullValue();
        DateWrapper dateWrapper = valueWrapper.asDate();
        return Date.valueOf(dateWrapper.toString());
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
        this.checkNullValue();
        TimeWrapper timeWrapper = valueWrapper.asTime();
        String time = String.format("%02d:%02d:%02d", timeWrapper.getHour(), timeWrapper.getMinute(), timeWrapper.getSecond());
        return Time.valueOf(time);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    public DateTimeWrapper getDateTime(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
        this.checkNullValue();
        return valueWrapper.asDateTime();
    }

    public DateTimeWrapper getDateTime(String columnLabel) throws SQLException {
        return getDateTime(findColumn(columnLabel));
    }

    public Node getNode(int columnIndex) throws SQLException {
        try {
            ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
            this.checkNullValue();
            return valueWrapper.asNode();
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("UnsupportedEncodingException occur in NebulaResult.getNode().", e);
        }
    }

    public Node getNode(String columnLabel) throws SQLException {
        return getNode(findColumn(columnLabel));
    }

    public Relationship getEdge(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
        this.checkNullValue();
        return valueWrapper.asRelationship();
    }

    public Relationship getEdge(String columnLabel) throws SQLException {
        return getEdge(findColumn(columnLabel));
    }

    public PathWrapper getPath(int columnIndex) throws SQLException {
        try {
            ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
            this.checkNullValue();
            return valueWrapper.asPath();
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("UnsupportedEncodingException occur in NebulaResult.getPath().", e);
        }
    }

    public PathWrapper getPath(String columnLabel) throws SQLException {
        return getPath(findColumn(columnLabel));
    }

    public List getList(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
        this.checkNullValue();
        return valueWrapper.asList();
    }

    public List getList(String columnLabel) throws SQLException {
        return getList(findColumn(columnLabel));
    }

    public Set getSet(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
        this.checkNullValue();
        try{
            return valueWrapper.asSet();
        }catch(InvalidValueException e){
            throw new SQLException(e);
        }
    }

    public Set getSet(String columnLabel) throws SQLException {
        return getSet(findColumn(columnLabel));
    }

    public Map getMap(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = (ValueWrapper) getObject(columnIndex);
        this.checkNullValue();
        HashMap<String, ValueWrapper> result = null;
        try {
            result = valueWrapper.asMap();
        } catch (UnsupportedEncodingException | InvalidValueException e) {
            throw new SQLException(e);
        }
        return result;
    }

    public Map getMap(String columnLabel) throws SQLException {
        return getMap(findColumn(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return NebulaResultSetMetaData.getInstance(this);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }



    public List<String> getColumnNames() {
        return columnNames;
    }
}
