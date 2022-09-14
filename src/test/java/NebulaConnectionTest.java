/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

import com.vesoft.nebula.jdbc.NebulaConnection;
import com.vesoft.nebula.jdbc.NebulaDriver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Attention: Please run {@link RunMeBeforeTest#createTestGraphSpace()} if you do not run that method before,
 * it will create graph space for test and insert data into it.
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NebulaConnectionTest {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    NebulaDriver driver;

    @BeforeAll
    public void getDriver() throws SQLException {
        driver = new NebulaDriver();
    }


    @Test
    public void getDefaultConnectionWithoutAuthenticationTest() throws SQLException {

        log.warn("make sure that you have set \"enable_authorize=true\" and \"auth_type=password\", otherwise the expected exception may not been thrown.");
        SQLException getDefaultConnectionWithoutAuthenticationException =
                assertThrows(SQLException.class, () -> DriverManager.getConnection(RunMeBeforeTest.URL));
        assertEquals("com.vesoft.nebula.client.graph.exception.AuthFailedException: Auth failed: Bad username/password",
                getDefaultConnectionWithoutAuthenticationException.getMessage());

    }

    @Test
    public void getDefaultConnectionWithAuthenticationTest() throws SQLException {

        Connection connection = DriverManager.getConnection(RunMeBeforeTest.URL, RunMeBeforeTest.USERNAME, RunMeBeforeTest.PASSWORD);
        assertTrue(connection instanceof NebulaConnection);
        connection.close();

    }

    @Test
    public void getCustomizedConnectionTest() throws SQLException {

        Properties connectionConfig = new Properties();
        connectionConfig.put("user", RunMeBeforeTest.USERNAME);
        connectionConfig.put("password", RunMeBeforeTest.PASSWORD);
        connectionConfig.put("reconnect", true);

        NebulaConnection connection = (NebulaConnection)DriverManager.getConnection(RunMeBeforeTest.URL, connectionConfig);
        assertEquals(connectionConfig.get("reconnect"), connection.getConnectionConfig().get("reconnect"));

        connection = (NebulaConnection)DriverManager.getConnection(RunMeBeforeTest.URL, RunMeBeforeTest.USERNAME, RunMeBeforeTest.PASSWORD);
        assertNotEquals(connectionConfig, connection.getConnectionConfig());

        connection.close();

    }

    @Test
    public void changeGraphSpaceTest() throws SQLException {

        NebulaConnection connection = (NebulaConnection)DriverManager.getConnection(RunMeBeforeTest.URL, RunMeBeforeTest.USERNAME, RunMeBeforeTest.PASSWORD);
        assertEquals("JDBC_TEST_SPACE", connection.getSchema());

        connection.setSchema("ANOTHER_JDBC_TEST_SPACE");
        assertEquals("ANOTHER_JDBC_TEST_SPACE", connection.getSchema());

        connection.close();

    }

}
