/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.utils;

import com.vesoft.nebula.client.graph.data.HostAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaJdbcUrlParser {
    private static final Logger logger = LoggerFactory.getLogger(NebulaJdbcUrlParser.class);

    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBC_NEBULA_PREFIX = JDBC_PREFIX + "nebula:";
    public static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_*\\-]+)");
    protected final static String DEFAULT_SPACE = "default";

    public static Properties parse(String jdbcUrl, Properties defaults) throws URISyntaxException {
        if (!jdbcUrl.startsWith(JDBC_NEBULA_PREFIX)) {
            throw new URISyntaxException(jdbcUrl, "'" + JDBC_NEBULA_PREFIX + "' prefix is " +
                    "mandatory.");
        }

        String uriString = jdbcUrl.substring(JDBC_PREFIX.length());
        URI uri = new URI(uriString);

        Properties jdbcProperties = parseUriQueryPart(uri.getQuery(), defaults);

        // parse graph space from url
        String path = uri.getPath();
        String graphSpace;
        if (path == null || path.isEmpty() || path.equals("/")) {
            String defaultSpace = defaults.getProperty("space");
            graphSpace = defaultSpace == null ? DEFAULT_SPACE : defaultSpace;
        } else {
            Matcher matcher = DB_PATH_PATTERN.matcher(path);
            if (matcher.matches()) {
                graphSpace = matcher.group(1);
            } else {
                throw new URISyntaxException("wrong space name path: " + path, uriString);
            }
        }
        jdbcProperties.put(NebulaPropertyKey.DBNAME.getKeyName(), graphSpace);

        // parse graph address from url
        int indexOfAddress = jdbcUrl.indexOf("//");
        int indexOfSpace = jdbcUrl.indexOf("/", indexOfAddress + 2);
        String address = jdbcUrl.substring(indexOfAddress, indexOfSpace);
        jdbcProperties.put(NebulaPropertyKey.DBADDRESS.getKeyName(), address);

        return jdbcProperties;
    }


    /**
     * parse the properties from url, which is defined with &
     * like: jdbc:nebula://127.0.0.1:9669/test?useUnicode=utf-8&useSSL=true
     */
    private static Properties parseUriQueryPart(String query, Properties defaults) {
        if (query == null) {
            return defaults;
        }

        Properties urlProps = new Properties(defaults);
        String[] queryKeyValues = query.split("&");
        for (String keyValue : queryKeyValues) {
            String[] kvTokens = keyValue.split("=");
            if (kvTokens.length == 2) {
                urlProps.put(kvTokens[0], kvTokens[1]);
            } else {
                logger.warn("cannot parse parameter pair: {}", keyValue);
            }
        }
        return urlProps;
    }


    /**
     * resolve NebulaGraph address from url
     */
    public static List<HostAddress> getAddresses(String url) throws SQLException {
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
}
