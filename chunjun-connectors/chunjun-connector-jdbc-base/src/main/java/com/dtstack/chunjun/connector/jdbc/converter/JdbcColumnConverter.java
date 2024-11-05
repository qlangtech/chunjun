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

package com.dtstack.chunjun.connector.jdbc.converter;

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.jdbc.sink.IFieldNamesAttachedStatement;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.types.RowKind;

import io.vertx.core.json.JsonArray;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

/** Base class for all converters that convert between JDBC object and Flink internal object. */
public class JdbcColumnConverter
        extends AbstractRowConverter<ResultSet, JsonArray, IFieldNamesAttachedStatement, LogicalType> {


//    public JdbcColumnConverter(RowType rowType) {
//        this(rowType, null);
//    }


    public JdbcColumnConverter(
            ChunJunCommonConf commonConf, int fieldCount, List<IDeserializationConverter> toInternalConverters
            , List<Pair<ISerializationConverter<IFieldNamesAttachedStatement>, LogicalType>> toExternalConverters) {
        super(fieldCount, toInternalConverters, toExternalConverters);
        this.setCommonConf(commonConf);
    }

    @Override
    protected final ISerializationConverter<IFieldNamesAttachedStatement> wrapIntoNullableExternalConverter(
            ISerializationConverter<IFieldNamesAttachedStatement> serializationConverter, LogicalType type) {
        return (val, index, statement, statPos) -> {
            if (val.isNullAt(index)) {
                statement.setNull(index);
            } else {
                serializationConverter.serialize(val, index, statement, statPos);
            }
        };
    }

//    @Override
//    protected ISerializationConverter<FieldNamedPreparedStatement>
//    wrapIntoNullableExternalConverter(
//            ISerializationConverter serializationConverter, LogicalType type) {
//
//        return (val, index, statement, statPos) -> {
//            if (val.isNullAt(index)) {
//                statement.setObject(index, null);
//            } else {
//                serializationConverter.serialize(val, index, statement, statPos);
//            }
//        };
//    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(ResultSet resultSet) throws Exception {
        List<FieldConf> fieldConfList = commonConf.getColumn();
        ColumnRowData result;
//        if (fieldConfList.size() == 1
//                && ConstantValue.STAR_SYMBOL.equals(fieldConfList.get(0).getName())) {
//            result = new ColumnRowData(this.getFieldCount());
//            for (int index = 0; index < this.getFieldCount(); index++) {
//                Object field = resultSet.getObject(index + 1);
//                AbstractBaseColumn baseColumn =
//                        (AbstractBaseColumn) toInternalConverters.get(index).deserialize(field);
//                result.addField(baseColumn);
//            }
//            return result;
//        }
        int converterIndex = 0;
        result = new ColumnRowData(fieldConfList.size());
        for (FieldConf fieldConf : fieldConfList) {
            try {
                AbstractBaseColumn baseColumn = null;
                if (StringUtils.isBlank(fieldConf.getValue())) {
                    Object field = resultSet.getObject(converterIndex + 1);

                    baseColumn =
                            (AbstractBaseColumn)
                                    toInternalConverters.get(converterIndex).deserialize(field);
                    converterIndex++;
                }
                result.addField(assembleFieldProps(fieldConf, baseColumn));
            } catch (Exception e) {
                throw new RuntimeException("fieldName:" + fieldConf.getName(), e);
            }
        }
        return result;
    }

    @Override
    public final IFieldNamesAttachedStatement toExternal(
            RowData rowData, IFieldNamesAttachedStatement fieldNamesAttachedStatement) throws Exception {

        // FieldNamedPreparedStatement statement = fieldNamesAttachedStatement.getFieldNamedPstmt();


        if (rowData.getRowKind() == RowKind.DELETE) {
            // 当执行删除时只where 部分只出现 主键部分
            List<String> stmtFields = fieldNamesAttachedStatement.getFieldNamedPstmtFields();
            final int stmtFieldsCount = stmtFields.size();
            int foundStmtFieldsCount = 0;
            int indexOf;
            List<FieldConf> fields = this.commonConf.getColumn();
            FieldConf field = null;
            for (int index = 0; index < fields.size(); index++) {
                field = fields.get(index);

                if ((indexOf = stmtFields.indexOf(field.getName())) > -1) {
                    toExternalConverters.get(index).serialize(rowData, index, fieldNamesAttachedStatement, indexOf);
                    foundStmtFieldsCount++;
                }
                if (foundStmtFieldsCount >= stmtFieldsCount) {
                    return fieldNamesAttachedStatement;
                }
            }
        } else {
            for (int index = 0; index < rowData.getArity(); index++) {
                try {
                    toExternalConverters.get(index).serialize(rowData, index, fieldNamesAttachedStatement, index);
                } catch (Throwable e) {
                    if (rowData instanceof GenericRowData) {
                        throw new IllegalStateException("index:" + index + " val:" + ((GenericRowData) rowData).getField(index), e);
                    }
                    throw e;
                }
            }
        }
        return fieldNamesAttachedStatement;
    }

//    @Override
//    protected IDeserializationConverter createInternalConverter(LogicalType type) {
//        return getRowDataValConverter(type);
//    }

    public static IDeserializationConverter getRowDataValConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> {
                    Integer v;
                    if (val instanceof Boolean) {
                        v = ((Boolean) val) ? 1 : 0;
                        // return new BooleanColumn(((Boolean) val));
                    } else {
                        v = ((Integer) val);
                    }
                    return new BigDecimalColumn(v.byteValue());
                };
            case SMALLINT:
            case INTEGER:
                return val -> {
                    return new BigDecimalColumn((Integer) val);
                };
            case INTERVAL_YEAR_MONTH:
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> {
                            YearMonthIntervalType yearMonthIntervalType =
                                    (YearMonthIntervalType) type;
                            switch (yearMonthIntervalType.getResolution()) {
                                case YEAR:
                                    return new BigDecimalColumn(
                                            Integer.parseInt(String.valueOf(val).substring(0, 4)));
                                case MONTH:
                                case YEAR_TO_MONTH:
                                default:
                                    throw new UnsupportedOperationException(
                                            "jdbc converter only support YEAR");
                            }
                        };
            case FLOAT:
                return val -> new BigDecimalColumn(new BigDecimal(val.toString()).floatValue());
            case DOUBLE:
                return val -> new BigDecimalColumn(new BigDecimal(val.toString()).doubleValue());
            case BIGINT:
                return val -> new BigDecimalColumn((new BigDecimal(val.toString()).longValue()));
            case DECIMAL:
                return val -> new BigDecimalColumn(new BigDecimal(val.toString()));
            case CHAR:
            case VARCHAR:
                return val -> new StringColumn(val.toString());
            case DATE:
                return val -> new SqlDateColumn((Date) val);
            case TIME_WITHOUT_TIME_ZONE:
                return val -> new TimeColumn((Time) val);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val ->
                                new TimestampColumn(
                                        (Timestamp) val, ((TimestampType) (type)).getPrecision());

            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn((byte[]) val);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

//    @Override
//    protected ISerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
//            LogicalType type) {
//        //  return createJdbcStatementValConverter(type,null);
//        throw new UnsupportedOperationException();
//    }

    public static ISerializationConverter<IFieldNamesAttachedStatement> createJdbcStatementValConverter(LogicalType type, RowData.FieldGetter valGetter) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement, statPos) -> {
//                    statement.setBoolean(
//                            index, ((ColumnRowData) val).getField(index).asBoolean());
                    statement.setBoolean(statPos, (Boolean) valGetter.getFieldOrNull(val) //val.getBoolean(index)
                    );
                };
            case TINYINT:
                return (val, index, statement, statPos) -> statement.setByte(statPos, (Byte) valGetter.getFieldOrNull(val) //val.getByte(index)
                );
            case SMALLINT: {
                return (val, index, statement, statPos) -> {
//                    short a = 0;
//                    a = val.getShort(index);
                    statement.setShort(statPos, (Short) valGetter.getFieldOrNull(val) //a
                    );
                };
            }
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, index, statement, statPos) -> {
                    // int a = 0;
//                    try {
//                        a = ((ColumnRowData) val).getField(index).asYearInt();
//                    } catch (Exception e) {
//                        LOG.error("val {}, index{}", val, index, e);
//                    }
                    // a = val.getInt(index);
                    statement.setInt(statPos, (Integer) valGetter.getFieldOrNull(val));
                };
            case FLOAT:
                return (val, index, statement, statPos) -> {
                    // statement.setFloat(index, ((ColumnRowData) val).getField(index).asFloat());
                    statement.setFloat(statPos, (Float) valGetter.getFieldOrNull(val) //val.getFloat(index)
                    );
                };
            case DOUBLE:
                return (val, index, statement, statPos) -> {
//                    statement.setDouble(
//                            index, ((ColumnRowData) val).getField(index).asDouble());
                    statement.setDouble(
                            statPos, (Double) valGetter.getFieldOrNull(val) //val.getDouble(index)
                    );
                };
            case BIGINT:
                return (val, index, statement, statPos) -> {
                    // statement.setLong(index, ((ColumnRowData) val).getField(index).asLong());
                    statement.setLong(statPos, (Long) valGetter.getFieldOrNull(val) //val.getLong(index)
                    );
                };
            case DECIMAL:
                return (val, index, statement, statPos) -> {
                    statement.setBigDecimal(
                            statPos, ((DecimalData) valGetter.getFieldOrNull(val)).toBigDecimal()
                            //  val.getDecimal(index, -1, -1).toBigDecimal()
                    );
                };
            case CHAR:
            case VARCHAR:
                return (val, index, statement, statPos) -> {
//                    statement.setString(
//                            index, ((ColumnRowData) val).getField(index).asString());
                    statement.setString(
                            statPos, (String) valGetter.getFieldOrNull(val) //val.getString(index).toString()
                    );
                };
            case DATE:
                return (val, index, statement, statPos) -> {
                    //  statement.setDate(index, ((ColumnRowData) val).getField(index).asSqlDate());
                    statement.setDate(statPos, (java.sql.Date) valGetter.getFieldOrNull(val));
                    // statement.setDate(index, Date.valueOf(LocalDate.ofEpochDay((Integer) valGetter.apply(val))) //  Date.valueOf(LocalDate.ofEpochDay(val.getInt(index)))
                    // );
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement, statPos) -> {
                    // statement.setTime(index, ((ColumnRowData) val).getField(index).asTime());
                    // val.getTimestamp(index, -1).toLocalDateTime();
                    // statement.setTime(index, ((ColumnRowData) val).getField(index).asTime());
                    // val.get
                    // throw new UnsupportedOperationException("index:" + index + ",val:" + val.toString());
//                    java.sql.Time time =
                    statement.setTime(statPos, (Time) valGetter.getFieldOrNull(val));
//                    statement.setTime(index, new Time((Integer) valGetter.apply(val)) //   new Time(val.getInt(index))
//                    );
                };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, statement, statPos) -> {
//                    statement.setTimestamp(
//                            index, ((ColumnRowData) val).getField(index).asTimestamp());
                    statement.setTimestamp(
                            statPos, ((Timestamp) valGetter.getFieldOrNull(val))  //val.getTimestamp(index, -1).toTimestamp()
                    );
                };

            case BINARY:
            case VARBINARY:
                return (val, index, statement, statPos) -> {
                    Object v = valGetter.getFieldOrNull(val);
                    if (v instanceof Byte) {
                        statement.setByte(statPos, (Byte) v);
                    } else {
                        statement.setBytes(statPos, (byte[]) v);
                    }
                };

            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
