/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.oracle.converter;

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import oracle.sql.TIMESTAMP;

import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleColumnConverter extends JdbcColumnConverter {

    public OracleColumnConverter(
            ChunJunCommonConf commonConf, int fieldCount
            , List<IDeserializationConverter> toInternalConverters
            , List<Pair<ISerializationConverter<FieldNamedPreparedStatement>, LogicalType>> toExternalConverters) {
        super(commonConf, fieldCount, toInternalConverters, toExternalConverters);
    }

//    public OracleColumnConverter(RowType rowType, ChunJunCommonConf commonConf) {
//        super(rowType, commonConf);
//    }


    public static IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> new BigDecimalColumn(((Integer) val).byteValue());
            case SMALLINT:
            case INTEGER:
                return val -> {
                    if (val instanceof BigDecimal) {
                        return new BigDecimalColumn((BigDecimal) val);
                    }
                    return new BigDecimalColumn((Integer) val);
                };
            case FLOAT: {
                return val -> {
                    if (val instanceof BigDecimal) {
                        return new BigDecimalColumn((BigDecimal) val);
                    }
                    return new BigDecimalColumn((Float) val);
                };
            }
            case DOUBLE:
                return val -> new BigDecimalColumn((Double) val);
            case BIGINT: {
                return val -> {
                    if (val instanceof BigDecimal) {
                        return new BigDecimalColumn((BigDecimal) val);
                    }
                    return new BigDecimalColumn((Long) val);
                };
            }
            case DECIMAL:
                return val -> new BigDecimalColumn((BigDecimal) val);
            case CHAR:
            case VARCHAR:
                // if (type instanceof ClobType) {
                return val -> {
                    if (val instanceof oracle.sql.CLOB) {
                        oracle.sql.CLOB clob = (oracle.sql.CLOB) val;
                        return new StringColumn(ConvertUtil.convertClob(clob));
                    } else {
                        return new StringColumn((String) val);
                    }
                };
            //}
            // return val -> new StringColumn((String) val);
            case DATE:
                return val -> new TimestampColumn((Timestamp) val, 0);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn(((TIMESTAMP) val).timestampValue());
            case BINARY:
            case VARBINARY:
                return val -> {
                    //if (type instanceof BlobType) {
                    if (val instanceof oracle.sql.BLOB) {
                        oracle.sql.BLOB blob = (oracle.sql.BLOB) val;
                        byte[] bytes = ConvertUtil.toByteArray(blob);
                        return new BytesColumn(bytes);
                    } else {
                        return new BytesColumn((byte[]) val);
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement>
    wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, statement) -> {
            if (val.isNullAt(index)) {
                //if (((ColumnRowData) val).getField(index) == null) {
                try {
                    final int sqlType =
                            JdbcTypeUtil.typeInformationToSqlType(
                                    TypeConversions.fromDataTypeToLegacyInfo(
                                            TypeConversions.fromLogicalToDataType(type)));
                    statement.setNull(index, sqlType);
                } catch (Exception e) {
                    statement.setObject(index, null);
                }
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
    }

//    @Override
//    protected ISerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
//            LogicalType type) {
//        switch (type.getTypeRoot()) {
//            case BOOLEAN:
//                return (val, index, statement) ->
//                        statement.setBoolean(
//                                index, ((ColumnRowData) val).getField(index).asBoolean());
//            case TINYINT:
//                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
//            case SMALLINT:
//            case INTEGER:
//                return (val, index, statement) ->
//                        statement.setInt(index, ((ColumnRowData) val).getField(index).asInt());
//            case FLOAT:
//                return (val, index, statement) ->
//                        statement.setFloat(index, ((ColumnRowData) val).getField(index).asFloat());
//            case DOUBLE:
//                return (val, index, statement) ->
//                        statement.setDouble(
//                                index, ((ColumnRowData) val).getField(index).asDouble());
//
//            case BIGINT:
//                return (val, index, statement) ->
//                        statement.setLong(index, ((ColumnRowData) val).getField(index).asLong());
//            case DECIMAL:
//                return (val, index, statement) ->
//                        statement.setBigDecimal(
//                                index, ((ColumnRowData) val).getField(index).asBigDecimal());
//            case CHAR:
//            case VARCHAR:
//                return (val, index, statement) -> {
//                    if (type instanceof ClobType) {
//                        try (StringReader reader =
//                                new StringReader(
//                                        ((ColumnRowData) val).getField(index).asString())) {
//                            statement.setClob(index, reader);
//                        }
//                    } else {
//                        statement.setString(
//                                index, ((ColumnRowData) val).getField(index).asString());
//                    }
//                };
//            case DATE:
//            case TIMESTAMP_WITH_TIME_ZONE:
//            case TIMESTAMP_WITHOUT_TIME_ZONE:
//                return (val, index, statement) ->
//                        statement.setTimestamp(
//                                index, ((ColumnRowData) val).getField(index).asTimestamp());
//
//            case BINARY:
//            case VARBINARY:
//                return (val, index, statement) -> {
//                    if (type instanceof BlobType) {
//                        try (InputStream is = new ByteArrayInputStream(val.getBinary(index))) {
//                            statement.setBlob(index, is);
//                        }
//                    } else {
//                        statement.setBytes(index, val.getBinary(index));
//                    }
//                };
//            default:
//                throw new UnsupportedOperationException("Unsupported type:" + type);
//        }
//    }
}
