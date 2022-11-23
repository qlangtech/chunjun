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
package com.dtstack.chunjun.util;

import com.dtstack.chunjun.converter.RawTypeConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.qlangtech.tis.plugin.ds.IColMetaGetter;

import java.util.Arrays;
import java.util.List;

/**
 * Date: 2021/04/07 Company: www.dtstack.com
 *
 * @author tudou
 */
public class TableUtil {
    /**
     * 获取TypeInformation
     */
    public static TypeInformation<RowData> getTypeInformation(
            DataType[] dataTypes, String[] fieldNames) {
        return InternalTypeInfo.of(getRowType(dataTypes, fieldNames));
    }

    /**
     * 获取RowType
     */
    public static RowType getRowType(DataType[] dataTypes, String[] fieldNames) {
        return RowType.of(
                Arrays.stream(dataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new),
                fieldNames);
    }
    /**
     * only using in data sync/integration
     */
    public static RowType createRowTypeByColsMeta(
            List<IColMetaGetter> colsMeta, RawTypeConverter converter) {
        return (RowType) createDataTypeByColsMeta(colsMeta, converter).getLogicalType();
    }

    public static TypeInformation<RowData> getTypeInformation(
            List<IColMetaGetter> colsMeta, RawTypeConverter converter) {

        DataType[] dataTypes = new DataType[colsMeta.size()];
        String[] fieldNames = new String[colsMeta.size()];
        int index = 0;
        for (IColMetaGetter cm : colsMeta) {
            fieldNames[index] = cm.getName();
            dataTypes[index] = converter.apply(cm);
            index++;
        }

        return getTypeInformation(dataTypes, fieldNames);
    }

    public static DataType createDataTypeByColsMeta(
            List<IColMetaGetter> colsMeta, RawTypeConverter converter) {
        TableSchema.Builder builder = TableSchema.builder();

        for (IColMetaGetter cm : colsMeta) {
            DataType dataType = converter.apply(cm);
            builder.add(TableColumn.physical(cm.getName(), dataType));
        }
//        for (int i = 0; i < colsMeta.size(); i++) {
//            DataType dataType = converter.apply(types.get(i));
//            builder.add(TableColumn.physical(fieldNames.get(i), dataType));
//        }
        return builder.build().toRowDataType();
    }

    /**
     * only using in data sync/integration
     *
     * @param fields List<FieldConf>, field information name, type etc.
     */
    public static RowType createRowType(List<IColMetaGetter> fields, RawTypeConverter converter) {
        return (RowType) createTableSchema(fields, converter).toRowDataType().getLogicalType();
    }

    /**
     * only using in data sync/integration
     *
     * @param fields List<FieldConf>, field information name, type etc.
     */
    public static TableSchema createTableSchema(
            List<IColMetaGetter> fields, RawTypeConverter converter) {
        TableSchema.Builder builder = TableSchema.builder();
        for (IColMetaGetter col : fields) {
            DataType dataType = converter.apply(col);
            builder.add(TableColumn.physical(col.getName(), dataType));
        }
        return builder.build();

    }
}
