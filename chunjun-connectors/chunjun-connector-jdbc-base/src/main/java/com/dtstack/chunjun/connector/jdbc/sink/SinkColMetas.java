package com.dtstack.chunjun.connector.jdbc.sink;


import com.qlangtech.tis.datax.IStreamTableMeta;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-01 15:18
 **/
public class SinkColMetas implements Serializable {
    //  private final IStreamTableMeta tableMeta;
    private final List<IColMetaGetter> cols;
    private transient Map<String, IColMetaGetter> name2ColMap;

    public SinkColMetas(IStreamTableMeta tableMeta) {
        //  this.tableMeta = tableMeta;
        this.cols = tableMeta.getColsMeta();
        //   this.name2ColMap = tableMeta.getColsMeta().stream().collect(Collectors.toMap((c) -> c.getName(), (c) -> c));
    }

    public List<IColMetaGetter> getCols() {
        return this.cols;
    }

    public Map<String, IColMetaGetter> getName2ColMap() {
        if (name2ColMap == null) {
            this.name2ColMap = cols.stream().collect(Collectors.toMap((c) -> c.getName(), (c) -> c));
        }
        return this.name2ColMap;
    }
}
