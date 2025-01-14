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

package com.dtstack.chunjun.util;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.jdbc.TableCols;

import com.qlangtech.tis.plugin.ds.ColMeta;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;

import java.util.List;

/**
 * @author chuixue
 * @create 2021-07-05 14:54
 * @description
 */
public class ColumnBuildUtil {

    /**
     * 同步任务如果用户配置了常量字段，则将其他非常量字段提取出来
     *
     * @param fieldList fieldList
     */
    public static List<IColMetaGetter> handleColumnList(
            List<FieldConf> fieldList,
            TableCols colsMeta) {
        return colsMeta.filterBy(fieldList);

//        if (fieldList.size() == 1
//                && StringUtils.equals(ConstantValue.STAR_SYMBOL, fieldList.get(0).getName())) {
//            return Pair.of(fullColumnList, fullColumnTypeList);
//        }

//        List<String> columnNameList = new ArrayList<>(fieldList.size());
//        List<String> columnTypeList = new ArrayList<>(fieldList.size());
//
//        for (FieldConf fieldConf : fieldList) {
//            if (fieldConf.getValue() == null) {
//                boolean find = false;
//                String name = fieldConf.getName();
//                if (fullColumnList.size() == 0) {
//                    columnNameList.add(name);
//                    columnTypeList.add(fieldConf.getType());
//                    find = true;
//                }
//                for (int i = 0; i < fullColumnList.size(); i++) {
//                    if (name.equalsIgnoreCase(fullColumnList.get(i))) {
//                        columnNameList.add(name);
//                        columnTypeList.add(fullColumnTypeList.get(i));
//                        find = true;
//                        break;
//                    }
//                }
//                if (!find) {
//                    throw new ChunJunRuntimeException(
//                            String.format(
//                                    "can not find field:[%s] in columnNameList:[%s]",
//                                    name, GsonUtil.GSON.toJson(fullColumnList)));
//                }
//            }
//        }
//        return Pair.of(columnNameList, columnTypeList);
    }
}
