package com.dtstack.chunjun.connector.jdbc.dialect;

import com.dtstack.chunjun.converter.ISerializationConverter;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-04-11 17:00
 **/
public class ExternalConverter<SinkT, T> {
    // example type: IFieldNamesAttachedStatement
    private final ISerializationConverter<SinkT> serConverter;
    // org.apache.flink.table.types.DataType
    public final T flinkType;
    private final com.qlangtech.tis.plugin.ds.DataType dataType;

    public ExternalConverter(
            ISerializationConverter<SinkT> serConverter
            , T flinkType, com.qlangtech.tis.plugin.ds.DataType dataType) {
        this.serConverter = serConverter;
        this.flinkType = flinkType;
        this.dataType = dataType;
    }

    public ISerializationConverter<SinkT> getSerConverter() {
        return serConverter;
    }

//    public LogicalType getLogicalType() {
//        return this.flinkType.getLogicalType();
//    }

    public com.qlangtech.tis.plugin.ds.DataType getDataType() {
        return this.dataType;
    }
}
