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

package com.dtstack.chunjun.connector.doris.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.doris.converter.DorisHttpRowConverter;
import com.dtstack.chunjun.connector.doris.converter.DorisJdbcRowConverter;
import com.dtstack.chunjun.connector.doris.options.DorisConf;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcDynamicTableSink;
import com.dtstack.chunjun.connector.jdbc.sink.SinkColMetas;
import com.dtstack.chunjun.connector.mysql.dialect.MysqlDialect;
import com.dtstack.chunjun.enums.EWriteMode;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CollectionUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-11-21
 */
public class DorisDynamicTableSink extends JdbcDynamicTableSink {

    private final TableSchema physicalSchema;

    private final DorisConf dorisConf;

    public DorisDynamicTableSink(TableSchema physicalSchema, DorisConf dorisConf) {
        super(
                dorisConf.setToJdbcConf(),
                new MysqlDialect(),
                physicalSchema,
                new DorisJdbcOutputFormatBuilder(new DorisJdbcOutputFormat( new SinkColMetas(()-> Collections.emptyList()))));
        this.physicalSchema = physicalSchema;
        this.dorisConf = dorisConf;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        String url = dorisConf.getUrl();

        BaseRichOutputFormatBuilder builder =
                StringUtils.isBlank(url)
                        ? httpBuilder(rowType, dorisConf)
                        : jdbcBuilder(rowType, dorisConf);

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), dorisConf.getParallelism());
    }

    private DorisHttpOutputFormatBuilder httpBuilder(RowType rowType, DorisConf dorisConf) {
        DorisHttpOutputFormatBuilder builder = new DorisHttpOutputFormatBuilder();
        builder.setColumns(Arrays.asList(physicalSchema.getFieldNames()));
        builder.setConfig(dorisConf);
        builder.setDorisOptions(dorisConf);
        builder.setRowConverter(DorisHttpRowConverter.create(rowType));
        return builder;
    }

    private DorisJdbcOutputFormatBuilder jdbcBuilder(RowType rowType, DorisConf dorisConf) {
        DorisJdbcOutputFormatBuilder builder =
                new DorisJdbcOutputFormatBuilder(new DorisJdbcOutputFormat(new SinkColMetas(()-> Collections.emptyList())));

        String[] fieldNames = tableSchema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            FieldConf field = new FieldConf();
            field.setName(fieldNames[i]);
            field.setType(rowType.getTypeAt(i).asSummaryString());
            field.setIndex(i);
            columnList.add(field);
        }
        jdbcConf.setColumn(columnList);
        jdbcConf.setMode(
                (CollectionUtil.isNullOrEmpty(jdbcConf.getUniqueKey()))
                        ? EWriteMode.INSERT.name()
                        : EWriteMode.UPDATE.name());

        builder.setConfig(dorisConf);
        builder.setJdbcDialect(jdbcDialect);
        builder.setJdbcConf(jdbcConf);
        builder.setRowConverter(DorisJdbcRowConverter.create(rowType));
        return builder;
    }

    @Override
    public DynamicTableSink copy() {
        return new DorisDynamicTableSink(physicalSchema, dorisConf);
    }

    @Override
    public String asSummaryString() {
        return "doris sink";
    }
}
