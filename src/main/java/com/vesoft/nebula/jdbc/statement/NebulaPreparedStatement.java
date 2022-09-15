/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface NebulaPreparedStatement extends PreparedStatement, NebulaStatement {
    void checkParamsNumber(int parameterIndex) throws SQLException;

    void insertParameter(int parameterIndex, Object obj) throws SQLException;
}
