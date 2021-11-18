/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.InvalidConfigException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class NebulaDriver extends NebulaAbstractDriver {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private NebulaPoolConfig nebulaPoolConfig;
    private NebulaPool nebulaPool = new NebulaPool();

    /**
     * NebulaDriver() is used to get and registerDriver a default Driver object with default properties to initialize {@link NebulaPool},
     * the default properties can be seen in {@link #setDefaultPoolProperties()}.
     * */
    public NebulaDriver() throws SQLException {
        this.setDefaultPoolProperties();
        this.initNebulaPool();
        DriverManager.registerDriver(this);
    }

    /**
     * NebulaDriver(Properties poolProperties) is used to get and registerDriver a customized Driver object with customized properties to initialize {@link NebulaPool},
     * all the properties of {@link NebulaPool} can be configured.
     * */
    public NebulaDriver(Properties poolProperties) throws SQLException {
        this.poolProperties = poolProperties;
        this.initNebulaPool();
        DriverManager.registerDriver(this);
    }

    /**
     * NebulaDriver(String url) is used to get and registerDriver a Driver object with default properties and a customized server address to initialize {@link NebulaPool},
     * this method cam be used when you just need to set a server address and the rest of other properties will be set to default value.
     * */
    public NebulaDriver (String address) throws SQLException {
        String[] addressInfo =  address.split(":");
        if(addressInfo.length != 2){
            throw new SQLException(String.format("url [%s] is invalid, please make sure your url match thr format: \"ip:port\".", address));
        }

        String ip = addressInfo[0];
        int port = Integer.parseInt(addressInfo[1]);
        List<HostAddress> customizedAddressList = Arrays.asList(new HostAddress(ip, port));

        this.poolProperties.put("addressList", customizedAddressList);
        initNebulaPool();

        DriverManager.registerDriver(this);
    }

    private void initNebulaPool() throws SQLException {

        List<HostAddress> defaultAddressList = Arrays.asList(new HostAddress("127.0.0.1", 9669));
        List<HostAddress> addressList = (List<HostAddress>) poolProperties.getOrDefault("addressList", defaultAddressList);

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

        try{
            long start = System.currentTimeMillis();
            this.nebulaPool.init(addressList, nebulaPoolConfig);
            long end = System.currentTimeMillis();
            log.info("NebulaPool.init(addressList, nebulaPoolConfig) use " +  (end - start) + " ms");
        }catch (UnknownHostException | InvalidConfigException e){
            throw new SQLException(e);
        }

    }

    protected Session getSessionFromNebulaPool() throws SQLException {

        String user = (String) connectionConfig.getOrDefault("user", "root");
        String password = (String) connectionConfig.getOrDefault("password", "nebula");
        boolean reconnect = (boolean) connectionConfig.getOrDefault("reconnect", false);

        Session nebulaSession;
        try{
            nebulaSession = nebulaPool.getSession(user, password, reconnect);
        }catch (NotValidConnectionException | IOErrorException | AuthFailedException e){
            throw new SQLException(e);
        }
        return nebulaSession;

    }

    public void closePool(){
        nebulaPoolConfig = null;
        nebulaPool.close();
        nebulaPool = null;
        log.info("NebulaDriver closed");
    }

    @Override
    public Connection connect(String url, Properties connectionConfig) throws SQLException {
        if(this.acceptsURL(url)){
            parseUrlProperties(url, connectionConfig);
            this.connectionConfig.put("url", url);
            String graphSpace = this.connectionConfig.getProperty("graphSpace");
            NebulaConnection JdbcConnection = new NebulaConnection(this, graphSpace);
            log.info("Get JDBCConnection succeeded");
            return JdbcConnection;
        }else {
            throw new SQLException("url: " + url + " is not accepted, " +
                    "url example: jdbc:nebula://graphSpace " +
                    "make sure your url match this format.");
        }
    }

    /**
     * get the Properties that used in NebulaDriver's constructor to specify addressList and parameters of {@link  NebulaPool}.
     */
    public Properties getPoolProperties(){
        return this.poolProperties;
    }

    /**
     * get the configuration of {@link  NebulaPool} that was used to get a {@link Session} object.
     */
    public NebulaPoolConfig getNebulaPoolConfig(){
        return this.nebulaPoolConfig;
    }

    public Properties getConnectionConfig(){
        return this.connectionConfig;
    }

    private void setDefaultPoolProperties(){
        Properties defaultPoolProperties = new Properties();
        String defaultIp = "127.0.0.1";
        int defaultPort = 9669;
        ArrayList<HostAddress> addressList = new ArrayList<>();
        addressList.add(new HostAddress(defaultIp, defaultPort));

        defaultPoolProperties.put("addressList", addressList);
        defaultPoolProperties.put("minConnsSize", 0);
        defaultPoolProperties.put("maxConnsSize", 10);
        defaultPoolProperties.put("timeout", 0);
        defaultPoolProperties.put("idleTime", 0);
        defaultPoolProperties.put("intervalIdle", -1);
        defaultPoolProperties.put("waitTime", 0);

        this.poolProperties = defaultPoolProperties;
    }

}
