/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class NebulaJdbc {
    public static void main(String[] args) {
        try {
            testJdbc();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void testJdbc() throws SQLException, ClassNotFoundException {
        Class.forName("com.vesoft.nebula.jdbc.NebulaDriver");

        Connection connection = null;
        ResultSet rs = null;
        try {
            connection = DriverManager.getConnection("jdbc:nebula://127.0.0.1:9669,127.0.0.1:9670/test", "root",
                    "nebula");
            rs = connection.createStatement().executeQuery("match (v:person) return v limit 1");
            while (rs.next()) {
                System.out.println(rs.getObject(1));
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (rs != null) {
                rs.close();
            }
        }
    }


    public static void testJdbcWithHikari() throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setPoolName("HikariCP pool");
        config.addDataSourceProperty("user", "root");
        config.addDataSourceProperty("password", "nebula");
        config.setJdbcUrl("jdbc:nebula://127.0.0.1:9669/test");

        HikariDataSource hikariDataSource = new HikariDataSource(config);
        Connection connection = null;
        Statement st = null;
        try {
            connection = hikariDataSource.getConnection();
            st = connection.createStatement();
            ResultSet rs = st.executeQuery("match (v:person) return v limit 1");
            // before get the ResultSet's content, we must execute rs.next() first. need to modify.
            if (rs.next()) {
                System.out.println(rs.getObject(1));
            }

        } catch (
                SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (st != null) {
                st.close();
            }
        }
    }
}
