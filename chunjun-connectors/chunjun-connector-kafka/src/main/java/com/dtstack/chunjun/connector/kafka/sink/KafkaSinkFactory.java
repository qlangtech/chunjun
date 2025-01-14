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
package com.dtstack.chunjun.connector.kafka.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.kafka.conf.KafkaConf;
import com.dtstack.chunjun.connector.kafka.converter.KafkaColumnConverter;
import com.dtstack.chunjun.connector.kafka.partitioner.CustomerFlinkPartition;
import com.dtstack.chunjun.connector.kafka.serialization.RowSerializationSchema;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

/**
 * Date: 2021/04/07 Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaSinkFactory extends SinkFactory {
    protected final KafkaConf kafkaConf;


//    public KafkaSinkFactory(SyncConf config) {
//        super(config);
//        Gson gson =
//                new GsonBuilder()
//                        .registerTypeAdapter(StartupMode.class, new StartupModeAdapter())
//                        .create();
//        GsonUtil.setTypeAdapter(gson);
//        kafkaConf = gson.fromJson(gson.toJson(config.getWriter().getParameter()), KafkaConf.class);
//        super.initCommonConf(kafkaConf);
//    }

    public KafkaSinkFactory(SyncConf config, KafkaConf kafkaConf) {
        super(config);
        this.kafkaConf = kafkaConf;
//        Gson gson =
//                new GsonBuilder()
//                        .registerTypeAdapter(StartupMode.class, new StartupModeAdapter())
//                        .create();
//        GsonUtil.setTypeAdapter(gson);
//        kafkaConf = gson.fromJson(gson.toJson(config.getWriter().getParameter()), KafkaConf.class);
        super.initCommonConf(kafkaConf);
    }

    @Override
    protected DataStreamSink<RowData> createOutput(
            DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat) {
        return createOutput(dataSet, outputFormat, this.getClass().getSimpleName().toLowerCase());
    }

    @Override
    protected DataStreamSink<RowData> createOutput(
            DataStream<RowData> dataSet, OutputFormat<RowData> outputFormat, String sinkName) {
        //Preconditions.checkNotNull(dataSet);
        Preconditions.checkNotNull(sinkName);

        Properties props = new Properties();
        props.putAll(kafkaConf.getProducerSettings());

        if (kafkaConf.isDataCompelOrder()) {
            Preconditions.checkState(
                    kafkaConf.getParallelism() <= 1,
                    "when kafka sink dataCompelOrder set true , Parallelism must 1.");
        }


        RowSerializationSchema rowSerializationSchema;
        if (!CollectionUtil.isNullOrEmpty(kafkaConf.getPartitionAssignColumns())) {
            Preconditions.checkState(
                    !CollectionUtil.isNullOrEmpty(kafkaConf.getTableFields()),
                    "when kafka sink set partitionAssignColumns , tableFields must set.");
            for (String field : kafkaConf.getPartitionAssignColumns()) {
                Preconditions.checkState(
                        kafkaConf.getTableFields().contains(field),
                        "["
                                + field
                                + "] field in partitionAssignColumns , but not in tableFields:["
                                + kafkaConf.getTableFields()
                                + "]");
            }
            rowSerializationSchema =
                    createRowSerializationSchema(KafkaColumnConverter.create(this.syncConf,
                            kafkaConf, kafkaConf.getPartitionAssignColumns(), getSerializationConverterFactory()));
        } else {
            rowSerializationSchema = createRowSerializationSchema(null);

        }

        KafkaProducer kafkaProducer = createKafkaProducer(props, rowSerializationSchema);

        if (dataSet != null) {
            DataStreamSink<RowData> dataStreamSink = dataSet.addSink(kafkaProducer);
            dataStreamSink.name(sinkName);

            return dataStreamSink;
        }

        return null;

    }

    protected KafkaProducer createKafkaProducer(Properties props, RowSerializationSchema rowSerializationSchema) {
        return new KafkaProducer(
                kafkaConf.getTopic(),
                rowSerializationSchema,
                props,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE,
                FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);
    }

    protected final RowSerializationSchema createRowSerializationSchema(KafkaColumnConverter keyConverter) {

        // Function<FieldConf, ISerializationConverter<Map<String, Object>>> serializationConverterFactory = getSerializationConverterFactory();
        SerializationSchema<RowData> valueSerialization = getValSerializationSchema();
        // Preconditions.checkNotNull(serializationConverterFactory, "serializationConverterFactory can not be null");
        return new RowSerializationSchema(
                kafkaConf,
                new CustomerFlinkPartition<>(),
                keyConverter
                // KafkaColumnConverter.create(this.syncConf, kafkaConf, serializationConverterFactory)
                , valueSerialization) {

        };
    }

    protected SerializationSchema<RowData> getValSerializationSchema() {
        throw new UnsupportedOperationException();
    }

    protected Function<FieldConf, ISerializationConverter<Map<String, Object>>> getSerializationConverterFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("kafka not support transform");
        }
        return createOutput(dataSet, null);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }
}
