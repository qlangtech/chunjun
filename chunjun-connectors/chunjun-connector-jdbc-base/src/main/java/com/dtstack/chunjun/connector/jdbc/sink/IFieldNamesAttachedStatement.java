package com.dtstack.chunjun.connector.jdbc.sink;

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-05-28 14:55
 **/
public interface IFieldNamesAttachedStatement {

    FieldNamedPreparedStatement getFieldNamedPstmt();

    List<String> getFieldNamedPstmtFields();

    default void setBoolean(int pos, Boolean val) throws SQLException {
        getFieldNamedPstmt().setBoolean(pos, val);
    }

    default void setByte(int pos, Byte val) throws SQLException {
        getFieldNamedPstmt().setByte(pos, val);
    }

    default void setShort(int pos, Short val) throws SQLException {
        getFieldNamedPstmt().setShort(pos, val);
    }

    default void setInt(int pos, Integer val) throws SQLException {
        getFieldNamedPstmt().setInt(pos, val);
    }

    default void setFloat(int pos, Float val) throws SQLException {
        getFieldNamedPstmt().setFloat(pos, val);
    }

    default void setDouble(int pos, Double val) throws SQLException {
        getFieldNamedPstmt().setDouble(pos, val);
    }

    default void setLong(int pos, Long val) throws SQLException {
        getFieldNamedPstmt().setLong(pos, val);
    }

    default void setBigDecimal(int pos, BigDecimal val) throws SQLException {
        getFieldNamedPstmt().setBigDecimal(pos, val);
    }

    default void setString(int pos, String val) throws SQLException {
        getFieldNamedPstmt().setString(pos, val);
    }


    default void setDate(int pos, Date val) throws SQLException {
        getFieldNamedPstmt().setDate(pos, val);
    }

    default void setTime(int pos, Time val) throws SQLException {
        getFieldNamedPstmt().setTime(pos, val);
    }

    default void setTimestamp(int pos, Timestamp val) throws SQLException {
        getFieldNamedPstmt().setTimestamp(pos, val);
    }

    default void setBytes(int pos, byte[] val) throws SQLException {
        getFieldNamedPstmt().setBytes(pos, val);
    }

    default void setObject(int pos, Object val) throws SQLException {
        getFieldNamedPstmt().setObject(pos, val);
    }
}
