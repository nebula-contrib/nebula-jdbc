/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

import com.vesoft.nebula.jdbc.NebulaDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Attention: Please run {@link RunMeBeforeTest#createTestGraphSpace()} if you do not run that method before,
 * it will create graph space for test and insert data into it.
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NebulaStatementTest {

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
    public void executeQueryTest() throws SQLException {

        String queryTestNode = "match (v:testNode) return v";
        ResultSet resultSet = statement.executeQuery(queryTestNode);

        assertNotNull(resultSet);
        assertTrue(resultSet.next());

    }

    @Test
    public void executeQueryWithNullResponseValueTest() throws SQLException {

        String queryTestNode = "match (v:testNode) return id(v) as id, v.testNode.notExist as notExist ORDER BY id ASC";
        ResultSet resultSet = statement.executeQuery(queryTestNode);

        assertTrue(resultSet.next());

        SQLException getNotExistColumnByIndexException = assertThrows(SQLException.class, () -> resultSet.getString("notExist"));
        assertEquals("The current value you accessing is __NULL__, please check your nGql", getNotExistColumnByIndexException.getMessage());

        assertTrue(resultSet.wasNull());

    }

    @Test
    public void executeUpdateTest() throws SQLException {

        String insertTestNode = "INSERT VERTEX testNode (theString, theInt, theDouble, theTrueBool, theFalseBool, theDate, theTime, theDatetime) VALUES " +
                "\"testNode_7\":(\"Young\", 20, , 12.56, true, false, date(\"1949-10-01\"), time(\"15:00:00.000\"), datetime(\"1949-10-01T15:00:00.000\")); ";
        String updateTestNode = "UPDATE VERTEX ON testNode \"testNode_7\" SET theInt = theInt + 2; ";
        String deleteTestNode = "DELETE VERTEX \"testNode_7\";";
        String queryTestNode = "MATCH (v:testNode) return id(v) AS id, v.testNode.theString AS theString, v.testNode.theInt AS theInt ORDER BY id ASC";

        int result = statement.executeUpdate(insertTestNode);
        assertEquals(0, result);
        // check whether insert the vertex successfully
        ResultSet resultSet = statement.executeQuery(queryTestNode);
        resultSet.absolute(7);
        assertEquals("Young", resultSet.getString("theString"));

        result = statement.executeUpdate(updateTestNode);
        assertEquals(0, result);
        // check whether update the vertex successfully
        resultSet = statement.executeQuery(queryTestNode);
        resultSet.absolute(7);
        assertEquals(22, resultSet.getInt(3));

        result = statement.executeUpdate(deleteTestNode);
        assertEquals(0, result);
        // check whether delete the vertex successfully
        resultSet = statement.executeQuery(queryTestNode);
        resultSet.absolute(6);
        assertFalse(resultSet.next());

    }

    @Test
    public void  executeTest() throws SQLException{

        String adminNGQL = "show hosts";
        boolean result = statement.execute(adminNGQL);
        assertTrue(result);

        ResultSet resultSet = statement.getResultSet();
        assertNotNull(resultSet);


        SQLException getResultSetHasBeenCalledException = assertThrows(SQLException.class, () -> statement.getResultSet());
        assertEquals("currentResultSet has been set to null, may have been called before.", getResultSetHasBeenCalledException.getMessage());

    }



}
