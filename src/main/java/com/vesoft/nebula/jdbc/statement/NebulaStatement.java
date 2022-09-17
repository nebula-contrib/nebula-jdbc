/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.statement;

import java.sql.Statement;
import java.sql.SQLException;

public interface NebulaStatement extends Statement {

    void checkReadOnly(String nGql) throws SQLException;

    void checkUpdate(String nGql) throws SQLException;

    void checkClosed() throws SQLException;

}
