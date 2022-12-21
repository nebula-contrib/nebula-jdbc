# nebula-jdbc

English | [中文版](/README-CN.md)

There are the version correspondence between client and Nebula:

| JDBC Branch | Nebula Graph Version |
|:-----------:|:--------------------:|
|    v2.5     |     2.5.0, 2.5.1     |
|   v2.6.2    | 2.6.0, 2.6.1, 2.6.2  |
|   v3.0.0    |        3.0.0         |
|   v3.3.0    |        3.3.0         |


## 0. Introduction

nebula-jdbc is encapsulated based on  [nebula-java](https://github.com/vesoft-inc/nebula-java), interfacing with the JDBC protocol and implementing JDBC-related interfaces. You may be more familiar with the JDBC API than with  [nebula-java](https://github.com/vesoft-inc/nebula-java). With nebula-jdbc you do not need to be familiar with the  [nebula-java](https://github.com/vesoft-inc/nebula-java) API (it is better if you are, so that you understand why our connection strings are formatted differently from the traditional JDBC connection strings) and you can manipulate the Nebula service in the same way as you manipulate relational databases in a java application

## 1. Architecture

Some of main classes and interfaces relationships in nebula-jdbc are as follows: (the blue solid line is the extends relationship between classes, the green solid line is the implements relationship between interfaces, the green dashed line is the implements relationship between abstract classes and interfaces, and the green dashed line is the implements relationship between the abstract class and the interface)

![图片](docs/architecture.png)

The user first registers the driver with `NebulaDriver`, which has a `NebulaPool` property for getting the `Session` to communicate with the database. There are two constructors available in `NebulaDriver`, a
no-argument constructor that configures the default NebulaPool, a constructors that take a `Properties` as parameter can customise the NebulaPool configuration, and the construction that take a `String` as parameter can just specify an address to connect and let the rest parameters to be set as default

After registering the driver, the user can get the `Connection` via `DriverManager::getConnection(String url)`, and the `NebulaConnection` constructor will get the `Session` from the `NebulaPool` in the `NebulaDriver` and then connects to the graph space specified in the url

Once the `Connection` is obtained, the user can get a `Statement` or `PreparedStatement` object through `Connection::createStatement` and `Connection::prepareStatement` to call the `executeQuery, executeUpdate, execute` methods to send commands to the database. The result of this command is encapsulated in `NebulaResult`, which can be used to obtain different types of data by calling the various methods with different return data type

## 2. Usage

### import
```xml
<dependency>
    <groupId>org.nebula-contrib</groupId>
    <artifactId>nebula-jdbc</artifactId>
    <version>$VERSION</version>
</dependency>
```

[Here](https://github.com/nebula-contrib/nebula-jdbc/wiki/Nebula-JDBC-Wiki#nebula-jdbc-version-mapping-to-nebula-graph-core) is the version mapping table
### example

```java
// Get and register the default NebulaDriver, the default connection address is 127.0.0.1:9669, the rest of the default parameters can be found in NebulaDriver::setDefaultPoolProperties()
NebulaDriver defaultDriver = new NebulaDriver();

// If you just need to specify an address to connect and let the rest parameters to be set as default, NebulaDriver (String address) can be used
NebulaDriver customizedUrlDriver = new NebulaDriver("192.168.66.116:9669");

// If you want to connect to a specific service address and customize the connection configuration, you can use a custom NebulaDriver by encapsulating the configuration parameters in a Properties object and call NebulaDriver::NebulaDriver(Properties poolProperties)
Properties poolProperties = new Properties();
ArrayList<HostAddress> addressList = new ArrayList<>();
addressList.add(new HostAddress("192.168.66.226", 9669));
addressList.add(new HostAddress("192.168.66.222", 9670));

poolProperties.put("addressList", addressList);
poolProperties.put("minConnsSize", 2);
poolProperties.put("maxConnsSize", 12);
poolProperties.put("timeout", 1015);
poolProperties.put("idleTime", 727);
poolProperties.put("intervalIdle", 1256);
poolProperties.put("waitTime", 1256);

NebulaDriver customizedDriver = new NebulaDriver(poolProperties);

// get Connection
Connection connection = DriverManager.getConnection("jdbc:nebula://JDBC_TEST_SPACE", "root", "nebula123");

// get Statement and execute
Statement statement = connection.createStatement();

String queryStatementNgql = "match (v:testNode) return v.theString as theString, v.theInt as theInt";
ResultSet queryStatementResult = statement.executeQuery(queryStatementNgql);

// get data from resultset
while (queryStatementResult.next()){
String theString = queryStatementResult.getString("theString");
int theInt = queryStatementResult.getInt(2);
}

String insertTestNode = "INSERT VERTEX testNode (theString, theInt, theDouble, theTrueBool, theFalseBool, theDate, theTime, theDatetime) VALUES " +
    "\"testNode_7\":(\"Young\", 20, , 12.56, true, false, date(\"1949-10-01\"), time(\"15:00:00.000\"), datetime(\"1949-10-01T15:00:00.000\")); ";
statement.executeUpdate(insertTestNode);

// get PreparedStatement, set configuration parameters and execute
String insertPreparedStatementNgql = "INSERT VERTEX testNode (theString, theInt, theDouble, theTrueBool, theFalseBool, theDate, theTime, theDatetime) VALUES " +
    "\"testNode_8\":(?, ?, ?, ?, ?, ?, ?, ?); ";
PreparedStatement insertPreparedStatement = connection.prepareStatement(insertPreparedStatementNgql);

insertPreparedStatement.setString(1, "YYDS");
insertPreparedStatement.setInt(2, 98);
insertPreparedStatement.setDouble(3, 12.56);
insertPreparedStatement.setBoolean(4, true);
insertPreparedStatement.setBoolean(5, false);
insertPreparedStatement.setDate(6, Date.valueOf("1949-10-01"));
insertPreparedStatement.setTime(7, Time.valueOf("15:00:00"));
// make a class cast and then call setDatetime
NebulaPreparedStatement nebulaPreparedStatement = (NebulaPreparedStatement) insertPreparedStatement;
nebulaPreparedStatement.setDatetime(8, new java.util.Date());

insertPreparedStatement.execute();

//  close connection
connection.close();
```

## 3. Q & A

- Don't I need to specify the connection address in the connection string "jdbc:nebula://graphSpace"?

As the address list is already configured in NebulaDriver (default or custom), the connection string does not need to specify an address, only the graph space is required

- Does PreparedStatement have a pre-compile function?

No, the server does not support it at the moment.

- What are the usage scenarios for `executeQuery`, `executeUpdate`, `execute`?

`executeQuery` is used only for querying data in Nebula, where nGql needs to contain the query keywords ["match", "lookup", "go", "fetch", "find", "subgraph"] and return the result `ResultSet`; `executeUpdate` is used for modifying data, where nGql needs to contain the modify keyword
["update", "delete", "insert", "upsert", "create", "drop", "alter", "rebuild"], returns `0` as result in all case; `execute` for other admin operations, and returns `true` if the execution succeeds.

- The result of executeUpdate is `0`. Why is it not the amount of data affected by the statement?

If the user inserts multiple points or edges in a single insert statement, some of them may succeed and some of them may not, but the server will only return to tell the user that it has failed, however, the user may actually be able to find out some of the data have inserted. So this method will just return `0`

- How should I get the node, edge and path in `ResultSet` after they are returned in the query statement?

Convert `ResultSet` to `NebulaResultSet`, then call getNode, getEdge, getPath; the same for list, set, and map.

