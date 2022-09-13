/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc.utils;

public enum NebulaPropertyKey {
    USER("user", false),
    PASSWORD("password", false),
    HOST("host", false),
    PORT("port", false),
    PROTOCOL("protocol", false),
    PATH("path", false),
    TYPE("type", false),
    ADDRESS("address", false),
    PRIORITY("priority", false),
    DBNAME("graphspace", false),
    DBADDRESS("graphAddress", false),
    MINCONNSSIZE("minConnsSize", false),
    MAXCONNSSIZE("maxConnsSize", false),
    TIMEOUT("timeout", false),
    IDLETIME("idleTime", false),
    INTERVALIDLE("intervalIdle", false),
    WAITTIME("waitTime", false);

    private String keyName;
    private boolean isCaseSensitive;

    private NebulaPropertyKey(String keyName, boolean isCaseSensitive) {
        this.keyName = keyName;
        this.isCaseSensitive = isCaseSensitive;
    }

    public String toString() {
        return this.keyName;
    }

    public String getKeyName() {
        return this.keyName;
    }


}
