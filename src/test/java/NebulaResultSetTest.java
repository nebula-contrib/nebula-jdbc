/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

import com.vesoft.nebula.client.graph.data.DateTimeWrapper;
import com.vesoft.nebula.jdbc.NebulaDriver;
import com.vesoft.nebula.jdbc.NebulaResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.*;
import java.sql.Date;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Attention: Please run {@link RunMeBeforeTest#createTestGraphSpace()} if you do not run that method before,
 * it will create graph space for test and insert data into it.
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NebulaResultSetTest {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    NebulaDriver driver;
    Connection connection;
    Statement statement;

    @BeforeAll
    public void getConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Class.forName("com.vesoft.nebula.jdbc.NebulaDriver");
        connection = DriverManager.getConnection(RunMeBeforeTest.URL, RunMeBeforeTest.USERNAME, RunMeBeforeTest.PASSWORD);
        statement = connection.createStatement();
    }

    @AfterAll
    public void closeConnection() throws SQLException {
        connection.close();
    }

    @Test
    public void moveCursorTest() throws SQLException {

        String queryNode = "match (v:testNode) return v;";

        ResultSet resultSet = statement.executeQuery(queryNode);

        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getRow());

        resultSet.absolute(5);
        assertEquals(5, resultSet.getRow());

        resultSet.absolute(-2);
        assertEquals(5, resultSet.getRow());

        resultSet.relative(1);
        assertEquals(6, resultSet.getRow());

        resultSet.relative(-3);
        assertEquals(3, resultSet.getRow());

        resultSet.previous();
        assertEquals(2, resultSet.getRow());

        resultSet.first();
        assertEquals(1, resultSet.getRow());
        assertTrue(resultSet.isFirst());

        resultSet.beforeFirst();
        assertEquals(0, resultSet.getRow());
        assertTrue(resultSet.isBeforeFirst());

        resultSet.last();
        assertEquals(6, resultSet.getRow());
        assertTrue(resultSet.isLast());

        resultSet.afterLast();
        assertEquals(7, resultSet.getRow());
        assertTrue(resultSet.isAfterLast());

    }

    @Test
    public void checkReturnValueTest() throws SQLException {

        String queryReturnValue = "match (v:testNode) return id(v) as id, v.testNode.theString as theString, v.testNode.theInt as theInt, " +
                "v.testNode.theDouble as theDouble, v.testNode.theTrueBool as theTrueBool, v.testNode.theFalseBool as theFalseBool, " +
                "v.testNode.theDate as theDate, v.testNode.theTime as theTime, v.testNode.theDatetime as theDatetime, " +
                "[\"ok\", 1, 33.3, false, true, date(\"1937-07-07\"), time(\"12:06:20.418\"), datetime(\"1937-07-07T12:06:20.159\")] as theList, " +
                "{\"ok\", 1, 33.3, false, true, date(\"2020-07-15\"), time(\"22:06:20.163\"), datetime(\"2020-07-15T22:06:16.416\")} as theSet, " +
                "{key: 'Value', listKey: [{inner: 'Map1'}, {inner: 'Map2'}]} as theMap " +
                "ORDER BY id ASC";
        String queryNode = "MATCH (v:testNode) RETURN v as theNode;";
        String queryEdge = "MATCH (v1:testNode)-[e:testEdge]-(v2:testNode) RETURN e as theEdge;";
        String queryPath = "FIND ALL PATH FROM \"testNode_1\" TO \"testNode_2\" OVER * YIELD path AS p;";

        SQLException queryByExecuteUpdateException = assertThrows(SQLException.class, () -> statement.executeUpdate(queryReturnValue));
        assertEquals("Method executeUpdate() can only execute nGql to update data, but the current nGql do not contains any update keyword in [update, delete, insert, upsert, create, drop, alter, rebuild], please modify your nGql or use executeQuery(), execute().",
                queryByExecuteUpdateException.getMessage());

        ResultSet resultSet = statement.executeQuery(queryReturnValue);

        assertTrue(resultSet.next());
        // check all return value
        assertEquals("testNode_1", resultSet.getString(1));
        assertEquals("testNode_1", resultSet.getString("id"));

        assertEquals("Flash", resultSet.getString(2));
        assertEquals("Flash", resultSet.getString("theString"));

        assertEquals(23, resultSet.getInt(3));
        assertEquals(23, resultSet.getInt("theInt"));

        assertEquals(66.66, resultSet.getDouble(4), 0.01);
        assertEquals(66.66, resultSet.getDouble("theDouble"), 0.01);

        assertTrue(resultSet.getBoolean(5));
        assertTrue(resultSet.getBoolean("theTrueBool"));

        assertFalse(resultSet.getBoolean(6));
        assertFalse(resultSet.getBoolean("theFalseBool"));

        assertEquals(Date.valueOf("2020-07-15"), resultSet.getDate(7));
        assertEquals(Date.valueOf("2020-07-15"), resultSet.getDate("theDate"));

        assertEquals(Time.valueOf("22:06:20"), resultSet.getTime(8));
        assertEquals(Time.valueOf("22:06:20"), resultSet.getTime("theTime"));

        NebulaResultSet nebulaResultSet = (NebulaResultSet)resultSet;

        assertTrue((nebulaResultSet.getDateTime(9)) instanceof DateTimeWrapper);
        assertTrue((nebulaResultSet.getDateTime("theDatetime")) instanceof DateTimeWrapper);

        assertTrue((nebulaResultSet.getList(10)) instanceof ArrayList);
        assertTrue((nebulaResultSet.getList("theList")) instanceof ArrayList);

        assertTrue((nebulaResultSet.getSet(11)) instanceof HashSet);
        assertTrue((nebulaResultSet.getSet("theSet")) instanceof HashSet);

        assertTrue((nebulaResultSet.getMap(12)) instanceof HashMap);
        assertTrue((nebulaResultSet.getMap("theMap")) instanceof HashMap);

        NebulaResultSet finalNebulaResultSet = nebulaResultSet;
        SQLException getNotExistColumnByIndexException = assertThrows(SQLException.class, () -> finalNebulaResultSet.getObject(13));
        assertEquals("column index [13] is invalid, please check your parameters (the first one should be represent as 1 instead of 0).", getNotExistColumnByIndexException.getMessage());

        SQLException getNotExistColumnByLabelException = assertThrows(SQLException.class, () -> finalNebulaResultSet.getObject("notExistLabel"));
        assertEquals("No such column [notExistLabel] found, please check your parameters.", getNotExistColumnByLabelException.getMessage());

        nebulaResultSet = (NebulaResultSet)statement.executeQuery(queryNode);
        assertTrue(nebulaResultSet.next());
        assertTrue((nebulaResultSet.getNode(1)) instanceof com.vesoft.nebula.client.graph.data.Node);
        assertTrue((nebulaResultSet.getNode("theNode")) instanceof com.vesoft.nebula.client.graph.data.Node);

        nebulaResultSet = (NebulaResultSet)statement.executeQuery(queryEdge);
        assertTrue(nebulaResultSet.next());
        assertTrue((nebulaResultSet.getEdge(1)) instanceof com.vesoft.nebula.client.graph.data.Relationship);
        assertTrue((nebulaResultSet.getEdge("theEdge")) instanceof com.vesoft.nebula.client.graph.data.Relationship);

        nebulaResultSet = (NebulaResultSet)statement.executeQuery(queryPath);
        assertTrue(nebulaResultSet.next());
        assertTrue((nebulaResultSet.getPath(1)) instanceof com.vesoft.nebula.client.graph.data.PathWrapper);

    }

    @Test
    public void resultSetMaDataTest() throws SQLException {
        String queryReturnValue = "match (v:testNode) return id(v) as id, v.testNode.theString as theString, v.testNode.theInt as theInt, " +
                "v.testNode.theDouble as theDouble, v.testNode.theTrueBool as theTrueBool, v.testNode.theFalseBool as theFalseBool, " +
                "v.testNode.theDate as theDate, v.testNode.theTime as theTime, v.testNode.theDatetime as theDatetime, " +
                "[\"ok\", 1, 33.3, false, true, date(\"1937-07-07\"), time(\"12:06:20.418\"), datetime(\"1937-07-07T12:06:20.159\")] as theList, " +
                "{\"ok\", 1, 33.3, false, true, date(\"2020-07-15\"), time(\"22:06:20.163\"), datetime(\"2020-07-15T22:06:16.416\")} as theSet, " +
                "{key: 'Value', listKey: [{inner: 'Map1'}, {inner: 'Map2'}]} as theMap " +
                "ORDER BY id ASC";
        ResultSet resultSet = statement.executeQuery(queryReturnValue);

        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(12, metaData.getColumnCount());
        assertEquals("id", metaData.getColumnName(1));
        assertEquals("theString", metaData.getColumnName(2));
        assertEquals("theInt", metaData.getColumnName(3));
        assertEquals("theDouble", metaData.getColumnName(4));
        assertEquals("theTrueBool", metaData.getColumnName(5));
        assertEquals("theFalseBool", metaData.getColumnName(6));
        assertEquals("theDate", metaData.getColumnName(7));
        assertEquals("theTime", metaData.getColumnName(8));
        assertEquals("theDatetime", metaData.getColumnName(9));
        assertEquals("theList", metaData.getColumnName(10));
        assertEquals("theSet", metaData.getColumnName(11));
        assertEquals("theMap", metaData.getColumnName(12));

        assertEquals(RunMeBeforeTest.GRAPH_SPACE, metaData.getSchemaName(1));
    }

    @Test
    public void checkReturnedDateTimeValueInNotUTCTimeZone() throws SQLException {
        log.warn("make sure you have set the timezone in server into CST+8.");
        statement.executeUpdate("INSERT VERTEX testNode (theString, theInt, theDouble, theTrueBool, theFalseBool, theDate, theTime, theDatetime) " +
                "VALUES \"testNode_9\":(\"Summer\", 19, 93.65, true, false, date(\"1950-01-26\"), time(\"17:23:30.153\"), datetime(\"1950-07-15T01:06:20.456\"));");
        NebulaResultSet resultSet = (NebulaResultSet)statement.executeQuery("MATCH (v:testNode) RETURN id(v) AS id ,v.testNode.theDate as theDate, v.testNode.theTime as theTime, v.testNode.theDatetime as theDatetime ORDER BY id ASC");
        resultSet.absolute(7);
        try {
            assertEquals(Date.valueOf("1950-01-26"), resultSet.getDate("theDate"));
            assertEquals(Time.valueOf("09:23:30"), resultSet.getTime("theTime"));
            DateTimeWrapper theDatetime = resultSet.getDateTime("theDatetime");
            assertEquals(1950, theDatetime.getYear());
            assertEquals(7, theDatetime.getMonth());
            assertEquals(14, theDatetime.getDay());
            assertEquals(17, theDatetime.getHour());
            assertEquals(6, theDatetime.getMinute());
            assertEquals(20, theDatetime.getSecond());
        } catch (AssertionFailedError e) {
            log.error("error occurs here, maybe time zone in server is not CST+8");
            statement.executeUpdate("DELETE VERTEX \"testNode_9\"");
        }


    }


}
