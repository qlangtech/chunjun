package com.dtstack.chunjun.connector.jdbc;

import com.dtstack.chunjun.conf.FieldConf;

import com.qlangtech.tis.plugin.ds.DataType;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-25 18:27
 **/
public class TableCols {
    List<ColMeta> colsMeta;

    public TableCols(List<ColMeta> colsMeta) {
        this.colsMeta = colsMeta;
    }

    public static TableCols create(List<FieldConf> fieldsCfg) {
        return null;
    }

    public List<ColMeta> getCols() {
        return this.colsMeta;
    }

    public List<ColMeta> filterBy(List<FieldConf> fieldList) {
        Set<String> fields = fieldList.stream().map((f) -> f.getName()).collect(Collectors.toSet());
        return colsMeta.stream().filter((col) -> {
            return fields.contains(col.name);
        }).collect(Collectors.toList());
    }

    /**
     * @author: 百岁（baisui@qlangtech.com）
     * @create: 2022-08-25 17:45
     **/
    public static class ColMeta {
        public final String name;
        public final DataType type;

        public ColMeta(String name, DataType type) {
            this.name = name;
            this.type = type;
        }
    }
}
