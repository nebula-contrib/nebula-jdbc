/**
 * @author Flash
 * @Email flash@m.scnu.edu.cn
 */

package com.vesoft.nebula.jdbc.impl;

import com.vesoft.nebula.jdbc.NebulaAbstractParameterMetaData;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class NebulaParameterMetaData extends NebulaAbstractParameterMetaData{

    private static NebulaParameterMetaData nebulaParameterMetaData = null;

    private NebulaParameterMetaData(NebulaPreparedStatement preparedStatement){
        super();
        this.preparedStatement = preparedStatement;
    }

    private NebulaParameterMetaData(){
        super();
    }

    public static ParameterMetaData getInstance(NebulaPreparedStatement preparedStatement){
        if (nebulaParameterMetaData == null){
            nebulaParameterMetaData = new NebulaParameterMetaData(preparedStatement);
        }
        return nebulaParameterMetaData;
    }

    /**  If you want to get MetaData after PreparedStatement changes, the method below should be called. */
    public static void release(){
        nebulaParameterMetaData = null;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return preparedStatement.getParametersNumber();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        Object parameter =  preparedStatement.getParameters().get(param);
        if(parameter != null){
            return parameter.getClass().getName();
        }
        return String.format("No such param with index [%d]", param);
    }

}
