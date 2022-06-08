# nebula-jdbc

中文版 | [English](/README.md)

| JDBC 分支 | Nebula Graph 版本 |
|:--------------:|:-------------------:|
|    v2.5     |     2.5.0, 2.5.1     |
|   v2.6.2    | 2.6.0, 2.6.1, 2.6.2  |
|   v3.0.0    |        3.0.0         |

## 0. Introduction

nebula-jdbc 是基于 [nebula-java](https://github.com/vesoft-inc/nebula-java) 封装的，在其基础上对接了 JDBC 协议，实现 JDBC 的相关接口。比起  [nebula-java](https://github.com/vesoft-inc/nebula-java) 你可能更加熟悉  JDBC  的 API，使用 nebula-jdbc 你可以不必熟悉 [nebula-java](https://github.com/vesoft-inc/nebula-java) 的 API(熟悉的话会更好，这样你会理解为什么我们连接字符串的格式是为什么与传统的 JDBC 连接字符串不同)，像在 java 程序中操作关系型数据库一样操作 Nebula 服务

## 1. Architecture

nebula-jdbc 主要的一些类和接口的关系如下：(蓝色实线是类之间的 extends 关系，绿色实线是接口之间的 implements 关系，绿色虚线是抽象类与接口之间的 implements 关系)

![图片](docs/architecture.png)

用户首先通过 `NebulaDriver` 注册驱动，其中有 `NebulaPool` 属性，用于获取 `Session` 与数据库通信。`NebulaDriver` 中提供三个构造函数，无参构造函数按照默认参数配置 `NebulaPool`，接收一个 `Properties` 类型参数的构造函数可以自定义 `NebulaPool` 配置，接收一个 `String` 类型参数的构造函数可以只指定连接地址，其余参数按照默认配置。

注册驱动后用户可以通过 `DriverManager::getConnection(String url)` 获取 `Connection`。在 `NebulaConnection` 的构造函数中会通过 `NebulaDriver` 中的 `NebulaPool` 获取 `Session` 接着连接到在 url 中指定的图空间(graphSpace)。

获取到 `Connection` 后用户可以通过 `Connection::createStatement` 和 `Connection::prepareStatement` 拿到 `Statement` 或者 `PreparedStatement` 对象，调用其中的 `executeQuery、executeUpdate、execute` 方法向数据库发送命令，数据库执行此命令后的结果会封装在 `NebulaResult` 中，再调用其中各种获取数据的方法可以得到不同数据类型的数据。

## 2. Usage

### 引入依赖
```xml
<dependency>
    <groupId>org.nebula-contrib</groupId>
    <artifactId>nebula-jdbc</artifactId>
    <version>$VERSION</version>
</dependency>
```
[这里](https://github.com/nebula-contrib/nebula-jdbc/wiki/Nebula-JDBC-Wiki#nebula-jdbc-version-mapping-to-nebula-graph-core)是版本对应表

### 使用示例

```java
// 获取并注册默认的 NebulaDriver，默认的连接地址是 127.0.0.1：9669，其余的默认参数可以查看 NebulaDriver::setDefaultPoolProperties()
NebulaDriver defaultDriver = new NebulaDriver();

// 如果只想设置连接地，其余参数按照默认配置，则可以使用 NebulaDriver (String address)
NebulaDriver customizedUrlDriver = new NebulaDriver("192.168.66.116:9669");

// 如果要连接特定的服务地址以及自定义连接配置，可以使用自定义 NebulaDriver：将配置参数封装在一个 Properties 对象中，然后调用 NebulaDriver::NebulaDriver(Properties poolProperties)
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

// 获取 Connection
Connection connection = DriverManager.getConnection("jdbc:nebula://JDBC_TEST_SPACE", "root", "nebula123");

// 获取 Statement 并执行
Statement statement = connection.createStatement();

String queryStatementNgql = "match (v:testNode) return v.theString as theString, v.theInt as theInt";
ResultSet queryStatementResult = statement.executeQuery(queryStatementNgql);

// 获取结果
while (queryStatementResult.next()){
String theString = queryStatementResult.getString("theString");
int theInt = queryStatementResult.getInt(2);
}

String insertTestNode = "INSERT VERTEX testNode (theString, theInt, theDouble, theTrueBool, theFalseBool, theDate, theTime, theDatetime) VALUES " +
    "\"testNode_7\":(\"Young\", 20, , 12.56, true, false, date(\"1949-10-01\"), time(\"15:00:00.000\"), datetime(\"1949-10-01T15:00:00.000\")); ";
statement.executeUpdate(insertTestNode);

// 获取 PreparedStatement，设置参数并执行
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
// 类型转换后再调用 setDatetime
NebulaPreparedStatement nebulaPreparedStatement = (NebulaPreparedStatement) insertPreparedStatement;
nebulaPreparedStatement.setDatetime(8, new java.util.Date());

insertPreparedStatement.execute();

// 关闭连接
connection.close();
```

## 3. Q & A

- 连接字符串"jdbc:nebula://graphSpace"中不用指定连接地址吗？

由于地址列表已经在 `NebulaDriver` 中配置(默认或自定义)，所以连接字符串不需要指定地址，只需要指定图空间。

- `PreparedStatement` 是否有预编译功能？

服务端暂不支持。

- `executeQuery`、`executeUpdate`、`execute` 的使用场景？

`executeQuery` 专门用于查询 Nebula 中的数据，此时 nGql 需包含查询关键字 ["match", "lookup", "go", "fetch", "find", "subgraph"]，返回查询结果 `ResultSet`；`executeUpdate` 用于修改数据，nGql 需包含修改关键字 ["update", "delete", "insert", "upsert", "create", "drop", "alter", "rebuild"]，返回查询结果 `0`；`execute` 用于其他 admin 操作，执行成功则返回查询 `true`。

- `executeUpdate` 的返回结果是0，为什么不是受到该语句影响的数据量？

目前服务端没有 updateCount 统计返回给用户。假如用户一条插入语句里面同时插入多个点或者多条边，这里面可能有部分成功，但服务端只会返回告诉用户失败了，但是其实用户可能能查到部分数据。统一返回0给用户。

- 查询语句中返回点、边、路径后在 Result 中应该如何获得？

将 `ResultSet` 转为 `NebulaResultSet`，然后调用 `getNode`、`getEdge`、`getPath`；对于列表、集合、映射也是如此。
