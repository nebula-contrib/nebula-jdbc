/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.*;
import com.vesoft.nebula.client.graph.exception.InvalidValueException;

import com.vesoft.nebula.jdbc.utils.ExceptionBuilder;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaResultSet implements java.sql.ResultSet {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public static final int SUPPORTED_TYPE = TYPE_SCROLL_INSENSITIVE;
    public static final int SUPPORTED_CONCURRENCY = CONCUR_READ_ONLY;
    public static final int SUPPORTED_HOLDABILITY = CLOSE_CURSORS_AT_COMMIT;

    protected com.vesoft.nebula.client.graph.data.ResultSet nativeNebulaResultSet;

    protected boolean isClosed = false;
    protected ValueWrapper lastColumnAccess;
    private int resultRowSize;
    private List<String> columnNames;
    private int currentRowNumber = -1;
    private ResultSet.Record currentRow;
    private Statement statement;

    public NebulaResultSet(ResultSet nebulaResultSet, Statement statement) {
        this.nativeNebulaResultSet = nebulaResultSet;
        this.statement = statement;
        if(!nativeNebulaResultSet.isEmpty()){
            this.resultRowSize = nativeNebulaResultSet.rowsSize();
            this.columnNames = nativeNebulaResultSet.getColumnNames();
        }
    }

    public com.vesoft.nebula.client.graph.data.ResultSet getNativeNebulaResultSet() {
        return nativeNebulaResultSet;
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

    @Override
    public int getType() throws SQLException {
        return SUPPORTED_TYPE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return SUPPORTED_CONCURRENCY;
    }

    @Override
    public int getHoldability() throws SQLException {
        return SUPPORTED_HOLDABILITY;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    /**
     * -----------------------Not implement yet-------------------------
     *
     * */

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }


    @Override
    public byte getByte(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }


    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
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
    public String getCursorName() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }


    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
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
    public boolean rowUpdated() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
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
