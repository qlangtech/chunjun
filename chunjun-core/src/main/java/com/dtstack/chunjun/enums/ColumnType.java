/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.enums;

import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataType.TypeVisitor;
import com.qlangtech.tis.plugin.ds.DataXReaderColType;

import org.apache.commons.lang.StringUtils;

/**
 * Define standard column type for all the readers or writers that do not have special types of
 * their own
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class ColumnType {
    public final DataType t;

    public ColumnType(DataType t) {
        this.t = t;
    }

    /**
     * @see java.sql.Types
     */
    public <T> T visit(TypeVisitor<T> visitor) {
        return t.accept(visitor);
    }

    //    /** string type */
//    STRING,
//    VARCHAR,
//    VARCHAR2,
//    CHAR,
//    NVARCHAR,
//    TEXT,
//    KEYWORD,
//    BINARY,
//
//    /** number type */
//    INT,
//    // for pg INT2,INT4,INT8
//    INT2,
//    INT4,
//    INT8,
//    INT16,
//    INT24,
//    INT32,
//    INT64,
//    UINT16,
//    UINT32,
//    UINT64,
//    SERIAL,
//    SERIAL8,
//    SMALLSERIAL,
//    BIGSERIAL,
//    DEC,
//    REAL,
//    MEDIUMINT,
//    TINYINT,
//    DATETIME,
//    SMALLINT,
//    BIGINT,
//    LONG,
//    SHORT,
//    INTEGER,
//    NUMBER,
//    NUMERIC,
//    MONEY,
//    OID,
//    DECIMAL,
//    BIGDECIMAL,
//
//    /** double type */
//    DOUBLE,
//    FLOAT,
//    FLOAT4,
//    FLOAT8,
//    FLOAT32,
//    FLOAT64,
//    SMALLFLOAT,
//    DECFLOAT,
//    BOOLEAN,
//    PRECISION,
//    DOUBLEPRECISION,
//
//    /** date type */
//    DATE,
//    TIMESTAMP,
//    TIME,
//    YEAR,
//    BIT,
//    OBJECT,
//    TIMESTAMPTZ;
//
//    public static List<ColumnType> TIME_TYPE = Arrays.asList(DATE, DATETIME, TIME, TIMESTAMP);
//
//    public static List<ColumnType> NUMBER_TYPE =
//            Arrays.asList(
//                    INT,
//                    INT16,
//                    INT24,
//                    INT32,
//                    INT64,
//                    UINT16,
//                    UINT32,
//                    UINT64,
//                    SERIAL,
//                    SERIAL8,
//                    SMALLSERIAL,
//                    BIGSERIAL,
//                    DEC,
//                    REAL,
//                    MONEY,
//                    OID,
//                    DECIMAL,
//                    BIGDECIMAL,
//                    INTEGER,
//                    MEDIUMINT,
//                    TINYINT,
//                    SMALLINT,
//                    BIGINT,
//                    LONG,
//                    SHORT,
//                    DOUBLE,
//                    FLOAT,
//                    FLOAT4,
//                    FLOAT8,
//                    FLOAT32,
//                    FLOAT64,
//                    SMALLFLOAT,
//                    DECFLOAT,
//                    DECIMAL,
//                    NUMBER,
//                    PRECISION,
//                    DOUBLEPRECISION);
//
//    public static List<ColumnType> STRING_TYPE =
//            Arrays.asList(STRING, VARCHAR, VARCHAR2, CHAR, NVARCHAR, TEXT, KEYWORD, BINARY);

    /**
     * 根据字段类型的字符串找出对应的枚举 找不到直接报错 IllegalArgumentException
     */
    public static ColumnType fromString(String type) {
        if (StringUtils.isEmpty(type)) {
            throw new IllegalArgumentException("param type can not be empty");
        }
        DataType t = DataType.ds(type);
        return new ColumnType(t);

//        if (type.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)) {
//            type = type.substring(0, type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL));
//        }
//
//        type = type.toUpperCase(Locale.ENGLISH);
//        // 为了支持无符号类型  如 int unsigned
//        if (StringUtils.contains(type, ConstantValue.DATA_TYPE_UNSIGNED)) {
//            type = type.replace(ConstantValue.DATA_TYPE_UNSIGNED, "").trim();
//        }
//        return valueOf(type);
    }

    /**
     * 根据字段类型的字符串找到对应的枚举 找不到就直接返回ColumnType.STRING;
     */
    public static ColumnType getType(String type) {
        return fromString(type);
//        type = type.toUpperCase(Locale.ENGLISH);
//        if (type.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)) {
//            type = type.substring(0, type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL));
//        }
//
//        // 为了支持无符号类型  如 int unsigned
//        if (StringUtils.contains(type, ConstantValue.DATA_TYPE_UNSIGNED)) {
//            type = type.replaceAll(ConstantValue.DATA_TYPE_UNSIGNED, "").trim();
//        }
//
//        if (type.contains(ColumnType.TIMESTAMP.name())) {
//            return TIMESTAMP;
//        }
//
//        for (ColumnType value : ColumnType.values()) {
//            if (type.equalsIgnoreCase(value.name())) {
//                return value;
//            }
//        }
//
//        return ColumnType.STRING;
    }

    public boolean isTimeType() {
        //  return TIME_TYPE.contains(getType(type));
        ColumnType t = this;//getType(type);
        return DataXReaderColType.Date == t.t.getCollapse();
    }

    public boolean isNumberType() {
        ColumnType t = this;//getType(type);
        DataXReaderColType ctype = t.t.getCollapse();
        return DataXReaderColType.INT == ctype || DataXReaderColType.Long == ctype || DataXReaderColType.Double == ctype;
        // return NUMBER_TYPE.contains(getType(type.replaceAll(" ", "")));
    }

    public boolean isStringType() {
        ColumnType t = this;//getType(type);
        return isStringType(t);
    }

    private static boolean isStringType(ColumnType type) {
        DataXReaderColType ctype = type.t.getCollapse();
        return DataXReaderColType.STRING == ctype;
    }
}
