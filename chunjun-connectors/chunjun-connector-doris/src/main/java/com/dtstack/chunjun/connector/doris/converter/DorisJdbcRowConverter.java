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

package com.dtstack.chunjun.connector.doris.converter;

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcRowConverter;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;



import java.util.List;

public class DorisJdbcRowConverter extends JdbcRowConverter {

    public DorisJdbcRowConverter(
            ChunJunCommonConf commonConf, int fieldCount, List<IDeserializationConverter> toInternalConverters
            , List<Pair<ISerializationConverter<FieldNamedPreparedStatement>, LogicalType>> toExternalConverters) {
        super(fieldCount, toInternalConverters, toExternalConverters);
        this.commonConf = commonConf;
    }

    public static AbstractRowConverter create(RowType rowType) {
        throw new UnsupportedOperationException();
    }

//    public DorisJdbcRowConverter(
//            ChunJunCommonConf commonConf, int fieldCount
//            , List<IDeserializationConverter> toInternalConverters, List<Pair<ISerializationConverter, LogicalType>> toExternalConverters) {
//        super(commonConf, fieldCount, toInternalConverters, toExternalConverters);
//    }

//    public DorisJdbcRowConverter(RowType rowType) {
//        super(rowType);
//    }
}
