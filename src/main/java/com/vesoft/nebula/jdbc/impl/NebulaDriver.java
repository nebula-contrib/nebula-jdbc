/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.*;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.jdbc.NebulaAbstractDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class NebulaDriver extends NebulaAbstractDriver {

    private static final Logger log = LoggerFactory.getLogger(NebulaDriver.class);

    private NebulaPoolConfig nebulaPoolConfig;
    private NebulaPool nebulaPool = new NebulaPool();

    static {
        try {
            NebulaDriver driver = new NebulaDriver();
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        log.info("Driver registered");
    }


    protected Session getSessionFromNebulaPool() throws SQLException {

        String user = (String) connectionConfig.getOrDefault("user", "root");
        String password = (String) connectionConfig.getOrDefault("password", "nebula");
        boolean reconnect = (boolean) connectionConfig.getOrDefault("reconnect", false);

        Session nebulaSession;
        try {
            nebulaSession = nebulaPool.getSession(user, password, reconnect);
        } catch (NotValidConnectionException | IOErrorException | AuthFailedException | ClientServerIncompatibleException e) {
            throw new SQLException(e);
        }
        return nebulaSession;

    }

    public void closePool() {
        nebulaPoolConfig = null;
        nebulaPool.close();
        nebulaPool = null;
        log.info("NebulaDriver closed");
    }

    /**
     * connect to the NebulaGraph
     */
    @Override
    public Connection connect(String url, Properties connectionConfig) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("url: " + url + " is not accepted, " +
                    "url example: jdbc:nebula://graphSpace " +
                    "make sure your url match this format.");

        }
        parseUrlProperties(url, connectionConfig);
        this.connectionConfig.put("url", url);
        String graphSpace = this.connectionConfig.getProperty("graphSpace");
        // todo reconstruct initPool
        initNebulaPool(url);
        NebulaConnection JdbcConnection = new NebulaConnection(this, graphSpace);
        log.info("Get JDBCConnection succeeded");
        return JdbcConnection;

    }

    /**
     * get the Properties that used in NebulaDriver's constructor to specify addressList and
     * parameters of {@link  NebulaPool}.
     */
    public Properties getPoolProperties() {
        return this.poolProperties;
    }

    /**
     * get the configuration of {@link  NebulaPool} that was used to get a {@link Session} object.
     */
    public NebulaPoolConfig getNebulaPoolConfig() {
        return this.nebulaPoolConfig;
    }

    public Properties getConnectionConfig() {
        return this.connectionConfig;
    }


    /**
     * resolve NebulaGraph address from url
     */
    private List<HostAddress> getAddresses(String url) throws SQLException {
        int startIdx = url.indexOf("/") + 2;
        int endIdx = url.lastIndexOf("/");

        String ipStr = url.substring(startIdx, endIdx);
        String[] ipStrs = ipStr.split(",");

        List<HostAddress> addressList = new ArrayList<>();
        for (int i = 0; i < ipStrs.length; i++) {
            String[] ipPort = ipStrs[i].split(":");
            if (ipPort.length < 2) {
                throw new SQLException("url has wrong host format.");
            }
            addressList.add(new HostAddress(ipPort[0], Integer.parseInt(ipPort[1])));
        }
        return addressList;
    }


    private void initNebulaPool(String url) throws SQLException {
        int minConnsSize = (int) poolProperties.getOrDefault("minConnsSize", 0);
        int maxConnsSize = (int) poolProperties.getOrDefault("maxConnsSize", 10);
        int timeout = (int) poolProperties.getOrDefault("timeout", 0);
        int idleTime = (int) poolProperties.getOrDefault("idleTime", 0);
        int intervalIdle = (int) poolProperties.getOrDefault("intervalIdle", -1);
        int waitTime = (int) poolProperties.getOrDefault("waitTime", 0);

        nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMinConnSize(minConnsSize);
        nebulaPoolConfig.setMaxConnSize(maxConnsSize);
        nebulaPoolConfig.setTimeout(timeout);
        nebulaPoolConfig.setIdleTime(idleTime);
        nebulaPoolConfig.setIntervalIdle(intervalIdle);
        nebulaPoolConfig.setWaitTime(waitTime);

        List<HostAddress> addressList = getAddresses(url);
        try {
            long start = System.currentTimeMillis();
            this.nebulaPool.init(addressList, nebulaPoolConfig);
            long end = System.currentTimeMillis();
            log.info("NebulaPool.init(addressList, nebulaPoolConfig) use " + (end - start) + " ms");
        } catch (UnknownHostException | InvalidConfigException e) {
            throw new SQLException(e);
        }
    }

}
