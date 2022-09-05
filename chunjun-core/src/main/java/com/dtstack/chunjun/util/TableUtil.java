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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.converter.RawTypeConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.qlangtech.tis.plugin.ds.ColMeta;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Date: 2021/04/07 Company: www.dtstack.com
 *
 * @author tudou
 */
public class TableUtil {

    /**
     * 获取TypeInformation
     *
     * @param fieldList 任务参数实体类
     *
     * @return TypeInformation
     */
    public static TypeInformation<RowData> getTypeInformation(
            List<FieldConf> fieldList, RawTypeConverter converter) {
        // throw new UnsupportedOperationException();
        com.qlangtech.tis.plugin.ds.DataType dataType = null;
        List<String> fieldName =
                fieldList.stream().map(FieldConf::getName).collect(Collectors.toList());
        if (fieldName.size() == 0) {
            return new GenericTypeInfo<>(RowData.class);
        }
        // com.qlangtech.tis.plugin.ds.DataType
        String[] fieldNames = fieldList.stream().map(FieldConf::getName).toArray(String[]::new);
//        String[] fieldTypes = fieldList.stream().map(FieldConf::getType).toArray(String[]::new);
        TableSchema.Builder builder = TableSchema.builder();
        for (FieldConf fileCfg : fieldList) {
            dataType = com.qlangtech.tis.plugin.ds.DataType.ds(fileCfg.getType());
//            DataType dataType = ;
            builder.add(TableColumn.physical(fileCfg.getName(), converter.apply(new ColMeta(fileCfg.getName(), dataType))));
        }
        DataType[] dataTypes =
                builder.build().toRowDataType().getChildren().toArray(new DataType[]{});

        return getTypeInformation(dataTypes, fieldNames);
    }

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


//    public static RowType createRowTypeByColsMeta(
//            List<String> fieldNames, List<String> types, RawTypeConverter converter) {
//        TableSchema.Builder builder = TableSchema.builder();
//        for (int i = 0; i < types.size(); i++) {
//            DataType dataType = converter.apply(types.get(i));
//            builder.add(TableColumn.physical(fieldNames.get(i), dataType));
//        }
//        return (RowType) builder.build().toRowDataType().getLogicalType();
//    }

    /**
     * only using in data sync/integration
     */
    public static RowType createRowTypeByColsMeta(
            List<ColMeta> colsMeta, RawTypeConverter converter) {
        TableSchema.Builder builder = TableSchema.builder();

        for (ColMeta cm : colsMeta) {
            DataType dataType = converter.apply(cm);
            builder.add(TableColumn.physical(cm.name, dataType));
        }
//        for (int i = 0; i < colsMeta.size(); i++) {
//            DataType dataType = converter.apply(types.get(i));
//            builder.add(TableColumn.physical(fieldNames.get(i), dataType));
//        }
        return (RowType) builder.build().toRowDataType().getLogicalType();
    }

    /**
     * only using in data sync/integration
     *
     * @param fields List<FieldConf>, field information name, type etc.
     */
    public static RowType createRowType(List<ColMeta> fields, RawTypeConverter converter) {
        return (RowType) createTableSchema(fields, converter).toRowDataType().getLogicalType();
    }

    /**
     * only using in data sync/integration
     *
     * @param fields List<FieldConf>, field information name, type etc.
     */
    public static TableSchema createTableSchema(
            List<ColMeta> fields, RawTypeConverter converter) {
//        String[] fieldNames = fields.stream().map(FieldConf::getName).toArray(String[]::new);
//        String[] fieldTypes = fields.stream().map(FieldConf::getType).toArray(String[]::new);
        TableSchema.Builder builder = TableSchema.builder();
        for (ColMeta col : fields) {
            DataType dataType = converter.apply(col);
            builder.add(TableColumn.physical(col.name, dataType));
        }
        return builder.build();

    }
}
