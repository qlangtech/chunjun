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

package com.dtstack.chunjun.connector.mysql.dialect;

import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.dialect.SupportUpdateMode;
import com.dtstack.chunjun.connector.mysql.converter.MysqlRawTypeConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.WriteMode;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @program: ChunJun
 * @author: wuren
 * @create: 2021/03/17
 */
@SupportUpdateMode(modes = {WriteMode.INSERT, WriteMode.UPSERT, WriteMode.UPDATE, WriteMode.REPLACE})
public class MysqlDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return MysqlRawTypeConverter::apply;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.mysql.jdbc.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    /**
     * Mysql upsert query use DUPLICATE KEY UPDATE.
     *
     * <p>NOTE: It requires Mysql's primary key to be consistent with pkFields.
     *
     * <p>We don't use REPLACE INTO, if there are other fields, we can keep their previous values.
     */
    @Override
    public Optional<String> getUpsertStatement(
            String schema,
            String tableName,
            List<String> fieldNames,
            List<String> uniqueKeyFields,
            boolean allReplace) {
        String updateClause;
        if (allReplace) {
            updateClause =
                    fieldNames.stream()
                            .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                            .collect(Collectors.joining(", "));
        } else {
            updateClause =
                    (fieldNames.stream())
                            .map(
                                    f ->
                                            quoteIdentifier(f)
                                                    + "=IFNULL(VALUES("
                                                    + quoteIdentifier(f)
                                                    + "),"
                                                    + quoteIdentifier(f)
                                                    + ")")
                            .collect(Collectors.joining(", "));
        }

        return Optional.of(
                getInsertIntoStatement(schema, tableName, fieldNames)
                        + " ON DUPLICATE KEY UPDATE "
                        + updateClause);
    }

    @Override
    public Optional<String> getReplaceStatement(
            String schema, String tableName, List<String> fieldNames) {
        String columns =
                (fieldNames.stream())
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                (fieldNames.stream()).map(f -> ":" + f).collect(Collectors.joining(", "));
        return Optional.of(
                "REPLACE INTO "
                        + buildTableInfoWithSchema(schema, tableName)
                        + "("
                        + columns
                        + ")"
                        + " VALUES ("
                        + placeholders
                        + ")");
    }

    /**
     * mysql 执行before 直接跳过
     */
    @Override
    public Optional<String> getUpdateBeforeStatement(String schema, String tableName, List<String> conditionFields) {
        return Optional.empty();
    }
}
