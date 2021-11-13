/**
 * @author Flash
 * @Email flash@m.scnu.edu.cn
 */

package com.vesoft.nebula.jdbc.utils;

import java.sql.SQLFeatureNotSupportedException;

public class ExceptionBuilder {

    private ExceptionBuilder() {}

    /**
     * An {@link SQLFeatureNotSupportedException} exception builder that  retrieve it's caller to make
     * a not yet implemented exception with method and class name.
     *
     * @return an SQLFeatureNotSupportedException
     */
    public static SQLFeatureNotSupportedException buildUnsupportedOperationException() {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        if (stackTraceElements.length > 2) {
            StackTraceElement caller = stackTraceElements[2];
            StringBuilder sb = new StringBuilder().append("Method ").append(caller.getMethodName()).append(" in class ").append(caller.getClassName())
                    .append(" is not supported.");
            return new SQLFeatureNotSupportedException(sb.toString());
        } else {
            return new SQLFeatureNotSupportedException("Not yet implemented.");
        }
    }

}
