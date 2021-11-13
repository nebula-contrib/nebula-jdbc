/**
 * @author Flash
 * @Email flash@m.scnu.edu.cn
 */

/**
 * @author Flash
 * @Email flash@m.scnu.edu.cn
 */

package com.vesoft.nebula.jdbc;

import com.vesoft.nebula.jdbc.impl.NebulaPreparedStatement;
import com.vesoft.nebula.jdbc.utils.ExceptionBuilder;

import java.sql.SQLException;

public abstract class NebulaAbstractParameterMetaData implements java.sql.ParameterMetaData{

    protected NebulaPreparedStatement preparedStatement;

    protected NebulaAbstractParameterMetaData() {

    }

    /**
     * -----------------------Not implement yet-------------------------
     *
     * */

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getScale(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }
}
