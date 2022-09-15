/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

import com.vesoft.nebula.jdbc.NebulaDriver;
import com.vesoft.nebula.jdbc.statement.NebulaPreparedStatementImpl;
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
public class NebulaPrepareStatementTest {

    NebulaDriver driver;

    @BeforeAll
    public void getDriver() throws SQLException {
        driver = new NebulaDriver();
    }


    @Test
    public void NebulaPrepareStatementTest() throws SQLException {
        Connection connection = DriverManager.getConnection(RunMeBeforeTest.URL, RunMeBeforeTest.USERNAME, RunMeBeforeTest.PASSWORD);

        String insert = "INSERT VERTEX testNode (theString, theInt, theDouble, theTrueBool, theFalseBool, theDate, theTime, theDatetime) VALUES " +
                "\"testNode_8\":(?, ?, ?, ?, ?, ?, ?, ?); ";
        String query = "MATCH (v:testNode) RETURN id(v) as id, v.testNode.theInt as theInt ORDER BY id ASC;";
        String update = "UPDATE VERTEX ON testNode \"testNode_8\" SET theInt = theInt + ?; ";
        String delete = "DELETE VERTEX ?; ";

        PreparedStatement insertPreparedStatement = connection.prepareStatement(insert);

        insertPreparedStatement.setString(1, "YYDS");
        insertPreparedStatement.setInt(2, 98);
        insertPreparedStatement.setDouble(3, 12.56);
        insertPreparedStatement.setBoolean(4, true);
        insertPreparedStatement.setBoolean(5, false);
        insertPreparedStatement.setDate(6, Date.valueOf("1919-09-09"));
        insertPreparedStatement.setTime(7, Time.valueOf("16:16:16"));
        NebulaPreparedStatementImpl nebulaPreparedStatement = (NebulaPreparedStatementImpl) insertPreparedStatement;
        nebulaPreparedStatement.setDatetime(8, new java.util.Date());

        ParameterMetaData parameterMetaData = insertPreparedStatement.getParameterMetaData();
        assertEquals(8, parameterMetaData.getParameterCount());
        assertEquals("java.lang.String", parameterMetaData.getParameterClassName(1));
        assertEquals("java.lang.Integer", parameterMetaData.getParameterClassName(2));
        assertEquals("java.lang.Double", parameterMetaData.getParameterClassName(3));
        assertEquals("java.lang.Boolean", parameterMetaData.getParameterClassName(4));
        assertEquals("java.lang.Boolean", parameterMetaData.getParameterClassName(5));
        assertEquals("java.sql.Date", parameterMetaData.getParameterClassName(6));
        assertEquals("java.sql.Time", parameterMetaData.getParameterClassName(7));
        assertEquals("java.util.Date", parameterMetaData.getParameterClassName(8));

        assertEquals(0, insertPreparedStatement.executeUpdate());

        PreparedStatement updatePreparedStatement = connection.prepareStatement(update);
        updatePreparedStatement.setInt(1, 2);
        assertEquals(0, updatePreparedStatement.executeUpdate());

        PreparedStatement queryPreparedStatement = connection.prepareStatement(query);
        ResultSet resultSet = queryPreparedStatement.executeQuery();
        resultSet.absolute(8);
        assertEquals(100, resultSet.getInt("theInt"));

        PreparedStatement deletePreparedStatement = connection.prepareStatement(delete);
        deletePreparedStatement.setString(1, "testNode_8");
        deletePreparedStatement.executeUpdate();

        connection.close();
    }



}
