///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.dtstack.chunjun.connector.kafka.table;
//
//import com.dtstack.chunjun.connector.kafka.sink.KafkaDynamicSink;
//import com.dtstack.chunjun.connector.kafka.source.KafkaDynamicSource;
//import com.dtstack.chunjun.connector.kafka.util.KafkaUtil;
//
//import org.apache.flink.api.common.serialization.DeserializationSchema;
//import org.apache.flink.api.common.serialization.SerializationSchema;
//import org.apache.flink.configuration.ConfigOption;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.configuration.ReadableConfig;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
//import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
//import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
//import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
//import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;
//import org.apache.flink.streaming.connectors.kafka.table.KafkaSinkSemantic;
//import org.apache.flink.table.api.ValidationException;
//import org.apache.flink.table.catalog.CatalogTable;
//import org.apache.flink.table.catalog.ObjectIdentifier;
//import org.apache.flink.table.connector.format.DecodingFormat;
//import org.apache.flink.table.connector.format.EncodingFormat;
//import org.apache.flink.table.connector.format.Format;
//import org.apache.flink.table.connector.sink.DynamicTableSink;
//import org.apache.flink.table.connector.source.DynamicTableSource;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.factories.DeserializationFormatFactory;
//import org.apache.flink.table.factories.DynamicTableSinkFactory;
//import org.apache.flink.table.factories.DynamicTableSourceFactory;
//import org.apache.flink.table.factories.FactoryUtil;
//import org.apache.flink.table.factories.SerializationFormatFactory;
//import org.apache.flink.table.types.DataType;
//import org.apache.flink.types.RowKind;
//
//import javax.annotation.Nullable;
//
//import java.time.Duration;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.Properties;
//import java.util.Set;
//import java.util.regex.Pattern;
//
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.KEY_FIELDS;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.KEY_FIELDS_PREFIX;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.KEY_FORMAT;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPERTIES_PREFIX;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_BOOTSTRAP_SERVERS;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_GROUP_ID;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_MODE;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_TOPIC_PARTITION_DISCOVERY;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_PARTITIONER;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_SEMANTIC;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC_PATTERN;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.VALUE_FIELDS_INCLUDE;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.VALUE_FORMAT;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.createKeyFormatProjection;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.createValueFormatProjection;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getFlinkKafkaPartitioner;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getSinkSemantic;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getStartupOptions;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.validateTableSinkOptions;
//import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.validateTableSourceOptions;
//import static org.apache.flink.table.factories.FactoryUtil.SCAN_PARALLELISM;
//import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
//
///**
// * @author chuixue
// * @create 2021-04-06 19:29
// * @description
// */
//public class KafkaDynamicTableFactory
//        implements DynamicTableSourceFactory, DynamicTableSinkFactory {
//
//    public static final String IDENTIFIER = "kafka-x";
//
//    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(
//            FactoryUtil.TableFactoryHelper helper) {
//        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
//                helper.discoverOptionalDecodingFormat(
//                        DeserializationFormatFactory.class, KEY_FORMAT);
//        keyDecodingFormat.ifPresent(
//                format -> {
//                    if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
//                        throw new ValidationException(
//                                String.format(
//                                        "A key format should only deal with INSERT-only records. "
//                                                + "But %s has a changelog mode of %s.",
//                                        helper.getOptions().get(KEY_FORMAT),
//                                        format.getChangelogMode()));
//                    }
//                });
//        return keyDecodingFormat;
//    }
//
//    private static Optional<EncodingFormat<SerializationSchema<RowData>>> getKeyEncodingFormat(
//            FactoryUtil.TableFactoryHelper helper) {
//        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
//                helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT);
//        keyEncodingFormat.ifPresent(
//                format -> {
//                    if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
//                        throw new ValidationException(
//                                String.format(
//                                        "A key format should only deal with INSERT-only records. "
//                                                + "But %s has a changelog mode of %s.",
//                                        helper.getOptions().get(KEY_FORMAT),
//                                        format.getChangelogMode()));
//                    }
//                });
//        return keyEncodingFormat;
//    }
//
//    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
//            FactoryUtil.TableFactoryHelper helper) {
//        return helper.discoverOptionalDecodingFormat(
//                        DeserializationFormatFactory.class, FactoryUtil.FORMAT)
//                .orElseGet(
//                        () ->
//                                helper.discoverDecodingFormat(
//                                        DeserializationFormatFactory.class, VALUE_FORMAT));
//    }
//
//    private static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
//            FactoryUtil.TableFactoryHelper helper) {
//        return helper.discoverOptionalEncodingFormat(
//                        SerializationFormatFactory.class, FactoryUtil.FORMAT)
//                .orElseGet(
//                        () ->
//                                helper.discoverEncodingFormat(
//                                        SerializationFormatFactory.class, VALUE_FORMAT));
//    }
//
//    private static void validatePKConstraints(
//            ObjectIdentifier tableName, CatalogTable catalogTable, Format format) {
//        if (catalogTable.getSchema().getPrimaryKey().isPresent()
//                && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
//            Configuration options = Configuration.fromMap(catalogTable.getOptions());
//            String formatName =
//                    options.getOptional(FactoryUtil.FORMAT).orElse(options.get(VALUE_FORMAT));
//            throw new ValidationException(
//                    String.format(
//                            "The Kafka table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
//                                    + " on the table, because it can't guarantee the semantic of primary key.",
//                            tableName.asSummaryString(), formatName));
//        }
//    }
//
//    // --------------------------------------------------------------------------------------------
//
//    @Override
//    public String factoryIdentifier() {
//        return IDENTIFIER;
//    }
//
//    @Override
//    public Set<ConfigOption<?>> requiredOptions() {
//        final Set<ConfigOption<?>> options = new HashSet<>();
//        options.add(PROPS_BOOTSTRAP_SERVERS);
//        return options;
//    }
//
//    @Override
//    public Set<ConfigOption<?>> optionalOptions() {
//        final Set<ConfigOption<?>> options = new HashSet<>();
//        options.add(FactoryUtil.FORMAT);
//        options.add(KEY_FORMAT);
//        options.add(KEY_FIELDS);
//        options.add(KEY_FIELDS_PREFIX);
//        options.add(VALUE_FORMAT);
//        options.add(VALUE_FIELDS_INCLUDE);
//        options.add(TOPIC);
//        options.add(TOPIC_PATTERN);
//        options.add(PROPS_GROUP_ID);
//        options.add(SCAN_PARALLELISM);
//        options.add(SCAN_STARTUP_MODE);
//        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
//        options.add(SCAN_TOPIC_PARTITION_DISCOVERY);
//        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
//        options.add(SINK_PARTITIONER);
//        options.add(SINK_SEMANTIC);
//        options.add(SINK_PARALLELISM);
//        return options;
//    }
//
//    @Override
//    public DynamicTableSource createDynamicTableSource(Context context) {
//        final FactoryUtil.TableFactoryHelper helper =
//                FactoryUtil.createTableFactoryHelper(this, context);
//
//        final ReadableConfig tableOptions = helper.getOptions();
//
//        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
//                getKeyDecodingFormat(helper);
//
//        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
//                getValueDecodingFormat(helper);
//
//        helper.validateExcept(PROPERTIES_PREFIX);
//
//        validateTableSourceOptions(tableOptions);
//
//        validatePKConstraints(
//                context.getObjectIdentifier(), context.getCatalogTable(), valueDecodingFormat);
//
//        final KafkaOptions.StartupOptions startupOptions = getStartupOptions(tableOptions);
//
//        final Properties properties =
//                KafkaUtil.getKafkaProperties(context.getCatalogTable().getOptions());
//
//        // add topic-partition discovery
//        properties.setProperty(
//                FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
//                String.valueOf(
//                        tableOptions
//                                .getOptional(SCAN_TOPIC_PARTITION_DISCOVERY)
//                                .map(Duration::toMillis)
//                                .orElse(FlinkKafkaConsumerBase.PARTITION_DISCOVERY_DISABLED)));
//
//        final DataType physicalDataType =
//                context.getCatalogTable().getSchema().toPhysicalRowDataType();
//
//        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);
//
//        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);
//
//        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);
//
//        final Integer parallelism = tableOptions.getOptional(SCAN_PARALLELISM).orElse(null);
//
//        return createKafkaTableSource(
//                physicalDataType,
//                keyDecodingFormat.orElse(null),
//                valueDecodingFormat,
//                keyProjection,
//                valueProjection,
//                keyPrefix,
//                KafkaOptions.getSourceTopics(tableOptions),
//                KafkaOptions.getSourceTopicPattern(tableOptions),
//                properties,
//                startupOptions.startupMode,
//                startupOptions.specificOffsets,
//                startupOptions.startupTimestampMillis,
//                parallelism);
//    }
//
//    @Override
//    public DynamicTableSink createDynamicTableSink(Context context) {
//        final FactoryUtil.TableFactoryHelper helper =
//                FactoryUtil.createTableFactoryHelper(this, context);
//
//        final ReadableConfig tableOptions = helper.getOptions();
//
//        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
//                getKeyEncodingFormat(helper);
//
//        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
//                getValueEncodingFormat(helper);
//
//        helper.validateExcept(PROPERTIES_PREFIX);
//
//        validateTableSinkOptions(tableOptions);
//
//        validatePKConstraints(
//                context.getObjectIdentifier(), context.getCatalogTable(), valueEncodingFormat);
//
//        final DataType physicalDataType =
//                context.getCatalogTable().getSchema().toPhysicalRowDataType();
//
//        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);
//
//        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);
//
//        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);
//
//        final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(null);
//
//        return createKafkaTableSink(
//                physicalDataType,
//                keyEncodingFormat.orElse(null),
//                valueEncodingFormat,
//                keyProjection,
//                valueProjection,
//                keyPrefix,
//                tableOptions.get(TOPIC).get(0),
//                KafkaUtil.getKafkaProperties(context.getCatalogTable().getOptions()),
//                getFlinkKafkaPartitioner(tableOptions, context.getClassLoader()).orElse(null),
//                getSinkSemantic(tableOptions),
//                parallelism);
//    }
//
//    // --------------------------------------------------------------------------------------------
//
//    protected KafkaDynamicSource createKafkaTableSource(
//            DataType physicalDataType,
//            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
//            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
//            int[] keyProjection,
//            int[] valueProjection,
//            @Nullable String keyPrefix,
//            @Nullable List<String> topics,
//            @Nullable Pattern topicPattern,
//            Properties properties,
//            StartupMode startupMode,
//            Map<KafkaTopicPartition, Long> specificStartupOffsets,
//            long startupTimestampMillis,
//            Integer parallelism) {
//        return new KafkaDynamicSource(
//                physicalDataType,
//                keyDecodingFormat,
//                valueDecodingFormat,
//                keyProjection,
//                valueProjection,
//                keyPrefix,
//                topics,
//                topicPattern,
//                properties,
//                startupMode,
//                specificStartupOffsets,
//                startupTimestampMillis,
//                false,
//                parallelism);
//    }
//
//    protected KafkaDynamicSink createKafkaTableSink(
//            DataType physicalDataType,
//            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
//            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
//            int[] keyProjection,
//            int[] valueProjection,
//            @Nullable String keyPrefix,
//            String topic,
//            Properties properties,
//            FlinkKafkaPartitioner<RowData> partitioner,
//            KafkaSinkSemantic semantic,
//            Integer parallelism) {
//        return new KafkaDynamicSink(
//                physicalDataType,
//                keyEncodingFormat,
//                valueEncodingFormat,
//                keyProjection,
//                valueProjection,
//                keyPrefix,
//                topic,
//                properties,
//                partitioner,
//                semantic,
//                false,
//                parallelism);
//    }
//}
