package com.dtstack.chunjun.connector.jdbc;

import com.dtstack.chunjun.conf.FieldConf;

import com.qlangtech.tis.plugin.ds.IColMetaGetter;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-25 18:27
 **/
public class TableCols {
    List<IColMetaGetter> colsMeta;

    public TableCols(List<IColMetaGetter> colsMeta) {
        this.colsMeta = colsMeta;
    }

    public static TableCols create(List<FieldConf> fieldsCfg) {
        return null;
    }

    public List<IColMetaGetter> getCols() {
        return this.colsMeta;
    }

    public List<IColMetaGetter> filterBy(List<FieldConf> fieldList) {
        Set<String> fields = fieldList.stream().map((f) -> f.getName()).collect(Collectors.toSet());
        return colsMeta.stream().filter((col) -> {
            return fields.contains(col.getName());
        }).collect(Collectors.toList());
    }
}
