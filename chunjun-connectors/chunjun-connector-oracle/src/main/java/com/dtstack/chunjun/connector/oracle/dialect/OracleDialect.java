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

package com.dtstack.chunjun.connector.oracle.dialect;

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.dialect.SupportUpdateMode;
import com.dtstack.chunjun.connector.jdbc.sink.IFieldNamesAttachedStatement;
import com.dtstack.chunjun.connector.oracle.converter.OracleColumnConverter;
import com.dtstack.chunjun.connector.oracle.converter.OracleRowConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.WriteMode;

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.types.logical.LogicalType;

import io.vertx.core.json.JsonArray;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
@SupportUpdateMode(modes = {WriteMode.INSERT, WriteMode.UPDATE, WriteMode.UPSERT})
public class OracleDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return "ORACLE";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:oracle:thin:");
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, IFieldNamesAttachedStatement, LogicalType> getColumnConverter(
            ChunJunCommonConf commonConf, int fieldCount, List<IDeserializationConverter> toInternalConverters
            , List<Pair<ISerializationConverter<IFieldNamesAttachedStatement>, LogicalType>> toExternalConverters) {
        return new OracleColumnConverter(commonConf, fieldCount, toInternalConverters, toExternalConverters);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        //  return OracleRawTypeConverter::apply;
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("oracle.jdbc.OracleDriver");
    }

    @Override
    public Optional<String> getReplaceStatement(
            String schema, String tableName, List<String> fieldNames) {
        throw new RuntimeException("Oracle does not support replace sql");
    }

    @Override
    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            List<String> fieldNames,
            List<String> uniqueKeyFields,
            boolean allReplace) {
        tableName = buildTableInfoWithSchema(schema, tableName);
        StringBuilder mergeIntoSql = new StringBuilder(64);
        mergeIntoSql
                .append("MERGE INTO ")
                .append(tableName)
                .append(" T1 USING (")
                .append(buildDualQueryStatement(fieldNames))
                .append(") T2 ON (")
                .append(buildEqualConditions(uniqueKeyFields))
                .append(") ");

        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);

        if (StringUtils.isNotEmpty(updateSql)) {
            mergeIntoSql.append(" WHEN MATCHED THEN UPDATE SET ");
            mergeIntoSql.append(updateSql);
        }

        mergeIntoSql
                .append(" WHEN NOT MATCHED THEN ")
                .append("INSERT (")
                .append(
                        (fieldNames.stream())
                                .map(this::quoteIdentifier)
                                .collect(Collectors.joining(", ")))
                .append(") VALUES (")
                .append(
                        (fieldNames.stream())
                                .map(col -> "T2." + quoteIdentifier(col))
                                .collect(Collectors.joining(", ")))
                .append(")");

        return Optional.of(mergeIntoSql.toString());
    }

//    @Override
//    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
//    getRowConverter(RowType rowType) {
//        return new OracleRowConverter(rowType);
//    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
    getRowConverter(
            int fieldCount, List<IDeserializationConverter> toInternalConverters
            , List<Pair<ISerializationConverter<FieldNamedPreparedStatement>, LogicalType>> toExternalConverters) {
        return new OracleRowConverter(fieldCount, toInternalConverters, toExternalConverters);
    }

    //    @Override
//    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
//    getColumnConverter(RowType rowType, ChunJunCommonConf commonConf) {
//        return new OracleColumnConverter(rowType, commonConf);
//    }

    /** build select sql , such as (SELECT ? "A",? "B" FROM DUAL) */
    public String buildDualQueryStatement(List<String> column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        String collect =
                column.stream()
                        .map(col -> ":" + col + " " + quoteIdentifier(col))
                        .collect(Collectors.joining(", "));
        sb.append(collect).append(" FROM DUAL");
        return sb.toString();
    }

    /** build sql part e.g: T1.`A` = T2.`A`, T1.`B` = T2.`B` */
    private String buildEqualConditions(List<String> uniqueKeyFields) {
        return (uniqueKeyFields.stream())
                .map(col -> "T1." + quoteIdentifier(col) + " = T2." + quoteIdentifier(col))
                .collect(Collectors.joining(" and "));
    }

    /** build T1."A"=T2."A" or T1."A"=nvl(T2."A",T1."A") */
    private String buildUpdateConnection(
            List<String> fieldNames, List<String> uniqueKeyFields, boolean allReplace) {
        //List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        return (fieldNames.stream())
                .filter(col -> !uniqueKeyFields.contains(col))
                .map(col -> buildConnectString(allReplace, col))
                .collect(Collectors.joining(","));
    }

    /**
     * Depending on parameter [allReplace] build different sql part. e.g T1."A"=T2."A" or
     * T1."A"=nvl(T2."A",T1."A")
     */
    private String buildConnectString(boolean allReplace, String col) {
        return allReplace
                ? "T1." + quoteIdentifier(col) + " = T2." + quoteIdentifier(col)
                : "T1."
                + quoteIdentifier(col)
                + " =NVL(T2."
                + quoteIdentifier(col)
                + ",T1."
                + quoteIdentifier(col)
                + ")";
    }

    @Override
    public String getRowNumColumn(String orderBy) {
        return "rownum as " + getRowNumColumnAlias();
    }
}
