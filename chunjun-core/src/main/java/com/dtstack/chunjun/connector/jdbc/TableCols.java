package com.dtstack.chunjun.connector.jdbc;

import com.dtstack.chunjun.conf.FieldConf;

import com.qlangtech.tis.plugin.ds.IColMetaGetter;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-25 18:27
 **/
public class TableCols<T extends IColMetaGetter> implements Serializable {
    List<T> colsMeta;
    private List<String> colKeys;

    public TableCols(List<T> colsMeta) {
        this.colsMeta = colsMeta;
        this.colKeys = colsMeta.stream().map((c) -> c.getName()).collect(Collectors.toList());
    }

    public static TableCols create(List<FieldConf> fieldsCfg) {
        return null;
    }

    public List<T> getCols() {
        return this.colsMeta;
    }

    public List<String> getColKeys() {
        return this.colKeys;
    }

    public List<IColMetaGetter> filterBy(List<FieldConf> fieldList) {
        Set<String> fields = fieldList.stream().map((f) -> f.getName()).collect(Collectors.toSet());
        return colsMeta.stream().filter((col) -> {
            return fields.contains(col.getName());
        }).collect(Collectors.toList());
    }
}
