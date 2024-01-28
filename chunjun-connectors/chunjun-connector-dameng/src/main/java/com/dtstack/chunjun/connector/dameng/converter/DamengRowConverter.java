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

package com.dtstack.chunjun.connector.dameng.converter;

import com.dtstack.chunjun.connector.jdbc.converter.JdbcRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class DamengRowConverter extends JdbcRowConverter {

    private static final long serialVersionUID = 1L;

    public static final int CLOB_LENGTH = 4000;

    protected ArrayList<IDeserializationConverter> toAsyncInternalConverters;

    public DamengRowConverter(
            int fieldCount, List<IDeserializationConverter> toInternalConverters
            , List<Pair<ISerializationConverter<FieldNamedPreparedStatement>, LogicalType>> toExternalConverters
    ) {
        super(fieldCount, toInternalConverters, toExternalConverters);
//        ArrayList<IDeserializationConverter> toAsyncInternalConverters
//        this.toAsyncInternalConverters = toAsyncInternalConverters;

        toAsyncInternalConverters = new ArrayList<>(fieldCount);
        for (Pair<ISerializationConverter<FieldNamedPreparedStatement>, LogicalType> p : toExternalConverters) {
            toAsyncInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createAsyncInternalConverter(p.getValue())));
        }

    }

//    public OracleRowConverter(RowType rowType) {
//        super(rowType);
//        toAsyncInternalConverters = new ArrayList<>(rowType.getFieldCount());
//        for (int i = 0; i < rowType.getFieldCount(); i++) {
//            toAsyncInternalConverters.add(
//                    wrapIntoNullableInternalConverter(
//                            createAsyncInternalConverter(rowType.getTypeAt(i))));
//        }
//    }

    protected IDeserializationConverter createAsyncInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                return val -> val;
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            default:
                return createInternalConverter(type);
        }
    }

    @Override
    public RowData toInternalLookup(JsonArray jsonArray) throws Exception {
        GenericRowData genericRowData = new GenericRowData(this.getFieldCount());
        for (int pos = 0; pos < this.getFieldCount(); pos++) {
            Object field = jsonArray.getValue(pos);
            genericRowData.setField(pos, toAsyncInternalConverters.get(pos).deserialize(field));
        }
        return genericRowData;
    }

    // @Override
    public static IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case BIGINT:
                return val -> Long.valueOf(val.toString());
            case INTEGER:
                return val -> Integer.valueOf(val.toString());
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val -> Long.valueOf(((Timestamp) val).getTime() / 1000).intValue();
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    if (val instanceof String) {
                        // oracle.sql.TIMESTAMP will return cast to String in lookup
                        return TimestampData.fromTimestamp(Timestamp.valueOf((String) val));
//                    } else if (val instanceof TIMESTAMP) {
//                        try {
//                            return TimestampData.fromTimestamp(((TIMESTAMP) val).timestampValue());
//                        } catch (SQLException e) {
//                            throw new UnsupportedTypeException(
//                                    "Unsupported type:" + type + ",value:" + val);
//                        }
                    } else {
                        return TimestampData.fromTimestamp(((Timestamp) val));
                    }
                };
            case CHAR:
                if (((CharType) type).getLength() > CLOB_LENGTH) {
                    return val -> {
                        Clob clob = (Clob) val;
                        return StringData.fromString(ConvertUtil.convertClob(clob));
                    };
                }
                return val -> StringData.fromString(val.toString());
            case VARCHAR:
                if (((VarCharType) type).getLength() > CLOB_LENGTH) {
                    return val -> {
                        Clob clob = (Clob) val;
                        return StringData.fromString(ConvertUtil.convertClob(clob));
                    };
                }
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> {
                    java.sql.Blob blob = (java.sql.Blob) val;
                    return ConvertUtil.toByteArray(blob);
                };
            default:
                throw new UnsupportedTypeException("Unsupported type:" + type);
        }
    }

}
