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

package com.dtstack.chunjun.connector.kafka.converter;

import com.dtstack.chunjun.conf.ContentConf;
import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.kafka.conf.KafkaConf;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.decoder.IDecode;
import com.dtstack.chunjun.decoder.JsonDecoder;

import org.apache.flink.table.data.RowData;

import com.google.common.collect.Maps;
import com.qlangtech.tis.plugin.ds.DataType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author chuixue
 * @create 2021-06-07 15:51
 * @description
 */
public class KafkaColumnConverter extends AbstractRowConverter<String, Object, Map<String, Object>, DataType> {

    /** source kafka msg decode */
    private final IDecode decode;
    /** sink json Decoder */
    private final JsonDecoder jsonDecoder;
    /** kafka Conf */
    private final KafkaConf kafkaConf;
    /** kafka sink out fields */
    private List<String> outList;

//    public static KafkaColumnConverter create(SyncConf syncConf, KafkaConf kafkaConf, List<String> keyTypeList) {
//        return create(syncConf, kafkaConf, Collections.emptyList(), serializationConverterFactory);
//    }

    public static KafkaColumnConverter create(
            SyncConf syncConf, KafkaConf kafkaConf
            , Function<FieldConf, ISerializationConverter<Map<String, Object>>> serializationConverterFactory) {
        return create(syncConf, kafkaConf, Collections.emptyList(), serializationConverterFactory);
    }


    public static KafkaColumnConverter create(
            SyncConf syncConf, KafkaConf kafkaConf, List<String> keyTypeList
            , Function<FieldConf, ISerializationConverter<Map<String, Object>>> serializationConverterFactory) {

        Objects.requireNonNull(serializationConverterFactory, "serializationConverterFactory can not be null");

        IDecode decode = null;
//        JsonDecoder jsonDecoder = null;
//        if (KafkaOptions.DEFAULT_CODEC.defaultValue().equals(kafkaConf.getCodec())) {
//            decode = new JsonDecoder();
//        } else {
//            decode = new TextDecoder();
//        }
        int fieldCount = 0;
        boolean shallFilterCols = CollectionUtils.isNotEmpty(keyTypeList);

        List<Pair<ISerializationConverter<Map<String, Object>>, DataType>> toExternalConverters = Lists.newArrayList();
        for (ContentConf contentConf : syncConf.getJob().getContent()) {
            List<FieldConf> fields = contentConf.getWriter().getFieldList();
            if (CollectionUtils.isEmpty(fields)) {
                throw new IllegalStateException("fieldList can not be empty");
            }
            fieldCount = fields.size();
            for (FieldConf field : fields) {
                if (shallFilterCols && !keyTypeList.contains(field.getName())) {
                    continue;
                }
                toExternalConverters.add(Pair.of(serializationConverterFactory.apply(field), field.getType()));
            }
            break;
        }

        if (CollectionUtils.isEmpty(toExternalConverters)) {
            throw new IllegalStateException("toExternalConverters can not be empty");
        }


        // Only json need to extract the fields
//        if (!CollectionUtils.isEmpty(kafkaConf.getColumn())
//                && KafkaOptions.DEFAULT_CODEC.defaultValue().equals(kafkaConf.getCodec())) {
//            List<String> typeList =
//                    kafkaConf.getColumn().stream()
//                            .map((c) -> (String) c.getType())
//                            .collect(Collectors.toList());
//            List<String> toInternalConverters = new ArrayList<>();
//            for (String s : typeList) {
//                toInternalConverters.add(
//                        wrapIntoNullableInternalConverter(createInternalConverter(s)));
//            }
//        }


        JsonDecoder jsonDecoder = null;


        return new KafkaColumnConverter(fieldCount, Collections.emptyList(), toExternalConverters, decode, jsonDecoder, kafkaConf);
    }


    private KafkaColumnConverter(
            int fieldCount, List<IDeserializationConverter> toInternalConverters
            , List<Pair<ISerializationConverter<Map<String, Object>>, DataType>> toExternalConverters, IDecode decode, JsonDecoder jsonDecoder, KafkaConf kafkaConf) {
        super(fieldCount, toInternalConverters, toExternalConverters);
        this.decode = decode;
        this.jsonDecoder = jsonDecoder;
        this.kafkaConf = kafkaConf;
    }

    public <T extends ISerializationConverter> List<T> getExternalConverters() {
        List<T> result = Lists.newArrayList();
        for (ISerializationConverter c : this.toExternalConverters) {
            result.add((T) c);
        }
        return result;
    }

    // Object Mapper is thread-safe
    //  private static final ObjectMapper OBJECT_MAPPER = initMapper();

//    private static ObjectMapper initMapper() {
//        final ObjectMapper result = new ObjectMapper().registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
//        result.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        result.configure(Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
//        return result;
//    }

    @Override
    public Map<String, Object> toExternal(RowData rowData, Map<String, Object> output) throws Exception {
        Map<String, Object> result = Maps.newHashMap();
//        for (int index = 0; index < rowData.getArity(); index++) {
//            toExternalConverters.get(index).serialize(rowData, index, result);
//        }

        for (ISerializationConverter serConverter : toExternalConverters) {
            serConverter.serialize(rowData, -1, result,-1);
        }

        return result;
//        final JsonNode value = jsonNode(result);
//        return value;
    }

//    public static <T> JsonNode jsonNode(final T object) {
//        return OBJECT_MAPPER.valueToTree(object);
//    }


//    public KafkaColumnConverter(KafkaConf kafkaConf, List<String> keyTypeList) {
//        this.kafkaConf = kafkaConf;
//        this.outList = keyTypeList;
//        this.jsonDecoder = new JsonDecoder();
//        if (DEFAULT_CODEC.defaultValue().equals(kafkaConf.getCodec())) {
//            this.decode = new JsonDecoder();
//        } else {
//            this.decode = new TextDecoder();
//        }
//    }
//
//    public KafkaColumnConverter(KafkaConf kafkaConf) {
//        this.commonConf = this.kafkaConf = kafkaConf;
//        this.jsonDecoder = new JsonDecoder();
//        if (DEFAULT_CODEC.defaultValue().equals(kafkaConf.getCodec())) {
//            this.decode = new JsonDecoder();
//        } else {
//            this.decode = new TextDecoder();
//        }
//
//        // Only json need to extract the fields
//        if (!CollectionUtils.isEmpty(kafkaConf.getColumn())
//                && DEFAULT_CODEC.defaultValue().equals(kafkaConf.getCodec())) {
//            List<String> typeList =
//                    kafkaConf.getColumn().stream()
//                            .map( (c)->  (String)c.getType()   )
//                            .collect(Collectors.toList());
//            this.toInternalConverters = new ArrayList<>();
//            for (String s : typeList) {
//                toInternalConverters.add(
//                        wrapIntoNullableInternalConverter(createInternalConverter(s)));
//            }
//        }
//    }

    @Override
    protected ISerializationConverter<Map<String, Object>> wrapIntoNullableExternalConverter(
            ISerializationConverter<Map<String, Object>> serializationConverter, DataType type) {
        return serializationConverter;
    }


//    @Override
//    protected ISerializationConverter<byte[]>
//    wrapIntoNullableExternalConverter(ISerializationConverter<byte[]> serializationConverter, String type) {
//
//        return super.wrapIntoNullableExternalConverter(serializationConverter, type);
//    }

//    protected ISerializationConverter<SinkT> wrapIntoNullableExternalConverter(
//            ISerializationConverter<SinkT> ISerializationConverter, T type) {
//        return null;
//    }


//    @Override
//    public byte[] toExternal(RowData rowData, byte[] output) throws Exception {
//
//
//        for (int index = 0; index < rowData.getArity(); index++) {
//            toExternalConverters.get(index).serialize(rowData, index, joiner);
//        }
//        return joiner;
//
//
//        Map<String, Object> map;
//        int arity = rowData.getArity();
//        ColumnRowData rows = (ColumnRowData) rowData;
//
//
//        for (int index = 0; index < rowData.getArity(); index++) {
//            toExternalConverters.get(index).serialize(rowData, index, joiner);
//        }
//        return joiner;
//
//        if (kafkaConf.getTableFields() != null
//                && kafkaConf.getTableFields().size() >= arity
//                && !(row.getField(0) instanceof MapColumn)) {
//            map = new LinkedHashMap<>((arity << 2) / 3);
//            for (int i = 0; i < arity; i++) {
//                Object object = row.getField(i);
//                Object value;
//                if (object instanceof TimestampColumn) {
//                    value = ((TimestampColumn) object).asTimestampStr();
//                } else {
//                    value = org.apache.flink.util.StringUtils.arrayAwareToString(row.getField(i));
//                }
//                map.put(kafkaConf.getTableFields().get(i), value);
//            }
//        } else {
//            String[] headers = row.getHeaders();
//            if (Objects.nonNull(headers) && headers.length >= 1) {
//                // cdc
//                map = new HashMap<>(headers.length >> 1);
//                for (String header : headers) {
//                    AbstractBaseColumn val = row.getField(header);
//                    if (null == val) {
//                        map.put(header, null);
//                    } else {
//                        map.put(header, val.getData());
//                    }
//                }
//                if (Arrays.stream(headers)
//                        .filter(
//                                i ->
//                                        i.equals(CDCConstantValue.BEFORE)
//                                                || i.equals(CDCConstantValue.AFTER)
//                                                || i.equals(CDCConstantValue.TABLE))
//                        .collect(Collectors.toSet())
//                        .size()
//                        == 3) {
//                    map = Collections.singletonMap("message", map);
//                }
//            } else if (row.getArity() == 1 && row.getField(0) instanceof MapColumn) {
//                // from kafka source
//                map = (Map<String, Object>) row.getField(0).getData();
//            } else {
//                List<String> values = new ArrayList<>(row.getArity());
//                for (int i = 0; i < row.getArity(); i++) {
//                    values.add(row.getField(i) == null ? "" : row.getField(i).asString());
//                }
//                map = decode.decode(String.join(",", values));
//            }
//        }
//
//        // get partition key value
//        if (!CollectionUtil.isNullOrEmpty(outList)) {
//            Map<String, Object> keyPartitionMap = new LinkedHashMap<>((arity << 2) / 3);
//            for (Map.Entry<String, Object> entry : map.entrySet()) {
//                if (outList.contains(entry.getKey())) {
//                    keyPartitionMap.put(entry.getKey(), entry.getValue());
//                }
//            }
//            map = keyPartitionMap;
//        }
//
//        return MapUtil.writeValueAsString(map).getBytes(StandardCharsets.UTF_8);
//    }

    @Override
    public RowData toInternal(String input) throws Exception {
        throw new UnsupportedOperationException();
//        Map<String, Object> map = decode.decode(input);
//        ColumnRowData result;
//        if (toInternalConverters == null || toInternalConverters.size() == 0) {
//            result = new ColumnRowData(1);
//            result.addField(new MapColumn(map));
//        } else {
//            List<FieldConf> fieldConfList = kafkaConf.getColumn();
//            result = new ColumnRowData(fieldConfList.size());
//            for (int i = 0; i < fieldConfList.size(); i++) {
//                FieldConf fieldConf = fieldConfList.get(i);
//                Object value = map.get(fieldConf.getName());
//                AbstractBaseColumn baseColumn =
//                        (AbstractBaseColumn) toInternalConverters.get(i).deserialize(value);
//                result.addField(assembleFieldProps(fieldConf, baseColumn));
//            }
//        }
//        return result;
    }

//    @Override
//    protected IDeserializationConverter createInternalConverter(String type) {
//        switch (type.toUpperCase(Locale.ENGLISH)) {
//            case "INT":
//            case "INTEGER":
//                return val -> new BigDecimalColumn(Integer.parseInt(val.toString()));
//            case "BOOLEAN":
//                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
//            case "TINYINT":
//                return val -> new BigDecimalColumn(Byte.parseByte(val.toString()));
//            case "CHAR":
//            case "CHARACTER":
//            case "STRING":
//            case "VARCHAR":
//            case "TEXT":
//                return val -> new StringColumn(val.toString());
//            case "SHORT":
//                return val -> new BigDecimalColumn(Short.parseShort(val.toString()));
//            case "LONG":
//            case "BIGINT":
//                return val -> new BigDecimalColumn(Long.parseLong(val.toString()));
//            case "FLOAT":
//                return val -> new BigDecimalColumn(Float.parseFloat(val.toString()));
//            case "DOUBLE":
//                return val -> new BigDecimalColumn(Double.parseDouble(val.toString()));
//            case "DECIMAL":
//                return val -> new BigDecimalColumn(new BigDecimal(val.toString()));
//            case "DATE":
//                return val -> new SqlDateColumn(Date.valueOf(val.toString()));
//            case "TIME":
//                return val -> new TimeColumn(Time.valueOf(val.toString()));
//            case "DATETIME":
//                return val -> new TimestampColumn(DateUtil.getTimestampFromStr(val.toString()), 0);
//            case "TIMESTAMP":
//                return val -> {
//                    String valStr = val.toString();
//                    try {
//                        return new TimestampColumn(
//                                Timestamp.valueOf(valStr),
//                                DateUtil.getPrecisionFromTimestampStr(valStr));
//                    } catch (Exception e) {
//                        return new TimestampColumn(DateUtil.getTimestampFromStr(valStr), 0);
//                    }
//                };
//            default:
//                throw new UnsupportedOperationException("Unsupported type:" + type);
//        }
//    }
}
