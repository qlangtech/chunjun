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

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.StringJoiner;
import java.util.function.Function;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-11-21
 */
public class DorisHttpRowConverter
        extends AbstractRowConverter<RowData, RowData, StringJoiner, LogicalType> {

    private static final long serialVersionUID = 2L;

    private static final String NULL_VALUE = "\\N";


    public static DorisHttpRowConverter create(RowType rowType) {
        throw new UnsupportedOperationException();
    }

    public DorisHttpRowConverter(
            int fieldCount, List<IDeserializationConverter> toInternalConverters
            , List<Pair<ISerializationConverter<StringJoiner>, LogicalType>> toExternalConverters) {
        super(fieldCount, toInternalConverters, toExternalConverters);
    }

//    public DorisHttpRowConverter(RowType rowType) {
//        super(rowType);
//        for (int i = 0; i < rowType.getFieldCount(); i++) {
//            toInternalConverters.add(
//                    wrapIntoNullableInternalConverter(
//                            createInternalConverter(rowType.getTypeAt(i))));
//            toExternalConverters.add(
//                    wrapIntoNullableExternalConverter(
//                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
//        }
//    }

    @Override
    public RowData toInternal(RowData input) throws Exception {
        return null;
    }

    @Override
    public StringJoiner toExternal(RowData rowData, StringJoiner joiner) throws Exception {
        for (int index = 0; index < this.getFieldCount(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, joiner, -1);
        }
        return joiner;
    }

    @Override
    protected ISerializationConverter<StringJoiner> wrapIntoNullableExternalConverter(
            ISerializationConverter<StringJoiner> ISerializationConverter, LogicalType type) {
        return ((rowData, index, joiner, statPos) -> {
            if (rowData == null
                    || rowData.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                joiner.add(NULL_VALUE);
            } else {
                ISerializationConverter.serialize(rowData, index, joiner, statPos);
            }
        });
    }

    // @Override
    public static ISerializationConverter<StringJoiner> createExternalConverter(LogicalType type, Function<RowData, Object> valGetter) {
        return (rowData, index, joiner, statPos) -> {
            // Object value = ((GenericRowData) rowData).getField(index);
            Object value = valGetter.apply(rowData);
            joiner.add("".equals(value.toString()) ? NULL_VALUE : value.toString());
        };
    }
}
