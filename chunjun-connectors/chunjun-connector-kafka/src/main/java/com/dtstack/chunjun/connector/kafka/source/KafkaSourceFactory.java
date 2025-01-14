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

package com.dtstack.chunjun.connector.kafka.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.kafka.conf.KafkaConf;
import com.dtstack.chunjun.connector.kafka.util.KafkaUtil;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;

import java.util.List;
import java.util.Properties;

/**
 * Date: 2019/11/21 Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaSourceFactory extends SourceFactory {

    /** kafka conf */
    protected KafkaConf kafkaConf;

    public KafkaSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env, List<IColMetaGetter> sourceColsMeta, RawTypeConverter typeConverter, KafkaConf kafkaConf) {
        super(syncConf, env, sourceColsMeta, typeConverter);
        this.kafkaConf = kafkaConf;
        super.initCommonConf(kafkaConf);
    }

//    public KafkaSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
//        super(config, env);
//        Gson gson =
//                new GsonBuilder()
//                        .registerTypeAdapter(StartupMode.class, new StartupModeAdapter())
//                        .create();
//        GsonUtil.setTypeAdapter(gson);
//        kafkaConf = gson.fromJson(gson.toJson(config.getReader().getParameter()), KafkaConf.class);
//        super.initCommonConf(kafkaConf);
//    }

    @Override
    public DataStream<RowData> createSource() {
        if (!useAbstractBaseColumn) {
            throw new UnsupportedOperationException("kafka not support transform");
        }
        Properties props = new Properties();
        props.put("group.id", kafkaConf.getGroupId());
        props.putAll(kafkaConf.getConsumerSettings());
        DynamicKafkaDeserializationSchema deserializationSchema =
                createKafkaDeserializationSchema(kafkaConf.getDeserialization());
        KafkaConsumerWrapper consumer =
                new KafkaConsumerWrapper(
                        Lists.newArrayList(kafkaConf.getTopic()), deserializationSchema, props);
        switch (kafkaConf.getMode()) {
            case EARLIEST:
                consumer.setStartFromEarliest();
                break;
            case LATEST:
                consumer.setStartFromLatest();
                break;
            case TIMESTAMP:
                consumer.setStartFromTimestamp(kafkaConf.getTimestamp());
                break;
            case SPECIFIC_OFFSETS:
                consumer.setStartFromSpecificOffsets(
                        KafkaUtil.parseSpecificOffsetsString(
                                kafkaConf.getTopic(), kafkaConf.getOffset()));
                break;
            default:
                consumer.setStartFromGroupOffsets();
                break;
        }
        consumer.setCommitOffsetsOnCheckpoints(kafkaConf.getGroupId() != null);
        return createInput(consumer, syncConf.getReader().getName());
    }

//    @Override
//    public RawTypeConverter getRawTypeConverter() {
//        return null;
//    }

    public DynamicKafkaDeserializationSchema createKafkaDeserializationSchema(String type) {

//        switch (type.toLowerCase(Locale.ENGLISH)) {
//            case "ticdc":
//                return new TicdcDeserializationSchema(kafkaConf);
//            default:
//                return new RowDeserializationSchema(kafkaConf, new KafkaColumnConverter(kafkaConf));
//        }
        throw new UnsupportedOperationException();
    }
}
