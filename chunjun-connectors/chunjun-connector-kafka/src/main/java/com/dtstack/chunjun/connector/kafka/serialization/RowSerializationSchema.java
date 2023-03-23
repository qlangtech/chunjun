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
package com.dtstack.chunjun.connector.kafka.serialization;

import com.dtstack.chunjun.connector.kafka.conf.KafkaConf;
import com.dtstack.chunjun.connector.kafka.converter.KafkaColumnConverter;
import com.dtstack.chunjun.connector.kafka.sink.DynamicKafkaSerializationSchema;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.MapUtil;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.RowData;

import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Date: 2021/03/04 Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class RowSerializationSchema extends DynamicKafkaSerializationSchema {


    private static final long serialVersionUID = 1L;
    // protected final Logger LOG = LoggerFactory.getLogger(getClass());
    protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    /** kafka key converter */
    private final KafkaColumnConverter keyConverter;
    /** kafka value converter */
    private final KafkaColumnConverter valueConverter;
    /** kafka converter */
    private final KafkaConf kafkaConf;


    public abstract Map<String, Object> createRowVals(String tableName, Map<String, Object> data);


    //  private final org.apache.kafka.connect.json.JsonSerializer serializer = new org.apache.kafka.connect.json.JsonSerializer();

    public RowSerializationSchema(
            KafkaConf kafkaConf,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            KafkaColumnConverter keyConverter,
            KafkaColumnConverter valueConverter) {
        super(kafkaConf.getTopic(), partitioner, null, null, null, null, false, null, false);
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.kafkaConf = kafkaConf;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        beforeOpen();
        LOG.info(
                "[{}] open successfully, \ncheckpointMode = {}, \ncheckpointEnabled = {}, \nflushIntervalMills = {}, \nbatchSize = {}, \n[{}]: \n{} ",
                this.getClass().getSimpleName(),
                checkpointMode,
                checkpointEnabled,
                0,
                1,
                kafkaConf.getClass().getSimpleName(),
                JsonUtil.toPrintJson(kafkaConf));
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(RowData element, @Nullable Long timestamp) {
        try {
            beforeSerialize(1, element);
            Map<String, Object> keySerialized = null;
            byte[] key = null;
            if (keyConverter != null) {
                keySerialized = keyConverter.toExternal(element, null);
                key = MapUtil.writeValueAsString(keySerialized).getBytes(StandardCharsets.UTF_8);
            }
            String valueSerialized = MapUtil.writeValueAsString(
                    createRowVals(kafkaConf.getTableName(), valueConverter.toExternal(element, null)));

            return new ProducerRecord<>(
                    this.topic,
                    extractPartition(element, key, null),
                    key,
                    valueSerialized.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            dirtyManager.collect(element, e, null);
        }
        return null;
    }
}
