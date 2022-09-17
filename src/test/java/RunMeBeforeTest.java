/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class RunMeBeforeTest {

    // you can modify driverProperties below according your config
    public static final String IP = "192.168.69.234";
    public static final int PORT = 9669;

    public static final String USERNAME = "root";
    public static final String PASSWORD = "nebula";

    public static final String JDBC_PREFIX = "jdbc:nebula://127.0.0.1:9669/";
    public static final String GRAPH_SPACE = "JDBC_TEST_SPACE";
    public static final String URL = JDBC_PREFIX + GRAPH_SPACE;

    public static final String SHOW_SPACES = "show spaces";
    public static final String CREATE_SPACE = "CREATE SPACE JDBC_TEST_SPACE(partition_num=15, replica_factor=1, vid_type=fixed_string(30));";
    public static final String CREATE_ANOTHER_SPACE = "CREATE SPACE ANOTHER_JDBC_TEST_SPACE(partition_num=15, replica_factor=1, vid_type=fixed_string(30));";

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Test
    public void createTestGraphSpace() throws SQLException, IOErrorException, AuthFailedException, NotValidConnectionException, UnknownHostException, InterruptedException, ClientServerIncompatibleException, ClassNotFoundException {

        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        List<HostAddress> addresses = Arrays.asList(new HostAddress(IP, PORT));
        NebulaPool pool = new NebulaPool();
        pool.init(addresses, nebulaPoolConfig);
        Session session = pool.getSession(USERNAME, PASSWORD, false);
        boolean JDBCTestSpaceNotExist = true;
        ResultSet result = session.execute(SHOW_SPACES);
        int spaceSize = result.rowsSize();
        for(int i = 0; i < spaceSize; ++i){
            if(RunMeBeforeTest.GRAPH_SPACE.equals(result.rowValues(i).get(0).toString())){
                JDBCTestSpaceNotExist = false;
            }
        }

        if(JDBCTestSpaceNotExist){
            ResultSet execute = session.execute(CREATE_SPACE);
            session.execute(CREATE_ANOTHER_SPACE);
            log.info("Wait a heartbeat cycle to make sure that graph space been created.");
            Thread.sleep(10001);
            if(execute.isSucceeded()){
                log.info("Graph space has been created.");
            }else{
                throw new SQLException(String.format("Graph space created failed, please check server status: [%s]", execute.getErrorMessage()));
            }
            session.release();
            pool.close();
        }

        Properties poolProperties = new Properties();
        ArrayList<HostAddress> addressList = new ArrayList<>();
        addressList.add(new HostAddress(RunMeBeforeTest.IP, RunMeBeforeTest.PORT));

        poolProperties.put("addressList", addressList);
        poolProperties.put("minConnsSize", 2);
        poolProperties.put("maxConnsSize", 12);
        poolProperties.put("timeout", 1015);
        poolProperties.put("idleTime", 727);
        poolProperties.put("intervalIdle", 1256);
        poolProperties.put("waitTime", 1256);

        // when customizedDriver init, it will registers a new instance of itself with the DriverManager.
        Class.forName("com.vesoft.nebula.jdbc.NebulaDriver");
        Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        Statement statement = connection.createStatement();

        String CREATE_TAG  = "CREATE TAG testNode(theString string, theInt int, theDouble double, theTrueBool bool, theFalseBool bool, theDate date, theTime time, theDatetime datetime)";
        String CREATE_EDGE = "CREATE EDGE testEdge(degree int);";
        String CREATE_NODE_INDEX = "CREATE TAG INDEX test_node_index on testNode();";
        String CREATE_EDGE_INDEX = "CREATE EDGE  INDEX test_edge_index on testEdge();";
        String INSERT_NODE = "INSERT VERTEX testNode (theString, theInt, theDouble, theTrueBool, theFalseBool, theDate, theTime, theDatetime) VALUES " +
                "\"testNode_1\":(\"Flash\", 23, 66.66, true, false, date(\"2020-07-15\"), time(\"22:06:20.163\"), datetime(\"2020-07-15T22:06:20.456\")), " +
                "\"testNode_2\":(\"Avery\", 23, 10.15, true, false, date(\"2020-07-15\"), time(\"22:06:21.143\"), datetime(\"2020-07-15T22:06:20.456\")), " +
                "\"testNode_3\":(\"Gitee\", 10, 93.65, true, false, date(\"1990-01-26\"), time(\"14:23:30.153\"), datetime(\"1950-07-15T22:06:20.456\")), " +
                "\"testNode_4\":(\"Graph\", 18, 12.56, true, false, date(\"1290-01-27\"), time(\"14:23:30.153\"), datetime(\"1290-07-15T22:06:20.456\")), " +
                "\"testNode_5\":(\"Space\", 45, 46.58, true, false, date(\"1180-07-27\"), time(\"17:36:30.130\"), datetime(\"1180-07-15T22:06:20.456\")), " +
                "\"testNode_6\":(\"Array\", 19, 93.65, true, false, date(\"1950-01-26\"), time(\"17:23:30.153\"), datetime(\"1950-07-15T22:06:20.456\"));";
        String INSERT_EDGE = "INSERT EDGE testEdge (degree) VALUES \"testNode_1\" -> \"testNode_2\":(10), \"testNode_1\" -> \"testNode_3\":(20), \"testNode_2\" -> \"testNode_3\":(30);";
        String REBUILD_NODE_INDEX = "REBUILD TAG INDEX test_node_index";
        String REBUILD_EDGE_INDEX = "REBUILD EDGE INDEX test_edge_index";

        statement.executeUpdate(CREATE_TAG);
        statement.executeUpdate(CREATE_EDGE);

        log.info("Tag is created asynchronously, wait a heartbeat cycle to perform subsequently.");
        Thread.sleep(10001);

        statement.executeUpdate(CREATE_NODE_INDEX);
        statement.executeUpdate(CREATE_EDGE_INDEX);

        Thread.sleep(10001);
        statement.executeUpdate(INSERT_NODE);
        statement.executeUpdate(INSERT_EDGE);

        Thread.sleep(10001);
        statement.executeUpdate(REBUILD_NODE_INDEX);
        statement.executeUpdate(REBUILD_EDGE_INDEX);

    }

}
