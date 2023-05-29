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
package com.dtstack.chunjun.connector.jdbc.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.converter.AbstractRowConverter;

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import org.apache.flink.types.RowKind;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * base on row data info to build preparedStatement. row data info include rowkind(which is to set
 * which sql kind to use )
 *
 * <p>Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-12-20
 */
public class DynamicPreparedStmt {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPreparedStmt.class);

    protected List<String> columnNameList;
//
//    protected List<String> columnTypeList = new ArrayList<>();

    protected transient FieldNamedPreparedStatement fieldNamedPreparedStatement;
    protected final JdbcConf jdbcConf;
    private boolean writeExtInfo;
    private JdbcDialect jdbcDialect;
    private AbstractRowConverter<?, ?, ?, ?> rowConverter;

    public DynamicPreparedStmt(JdbcConf jdbcConf) {
        this.jdbcConf = Objects.requireNonNull(jdbcConf, "jdbcConf can not be null");
    }


    public static Optional<DynamicPreparedStmt> buildStmt(
            JdbcConf jdbcConf,
            String schemaName,
            String tableName,
            RowKind rowKind,
            Connection connection,
            JdbcDialect jdbcDialect,
            List<FieldConf> fieldConfList,
            AbstractRowConverter<?, ?, ?, ?> rowConverter)
            throws SQLException {

        DynamicPreparedStmt dynamicPreparedStmt = new DynamicPreparedStmt(jdbcConf);
        dynamicPreparedStmt.jdbcDialect = jdbcDialect;
        dynamicPreparedStmt.rowConverter = rowConverter;
        //  String[] fieldNames = new String[fieldConfList.size()];
        //  FieldConf fieldConf = null;
//        List<String> columnNameList = Lists.newArrayList();
//        for (int i = 0; i < fieldConfList.size(); i++) {
//             fieldConf = fieldConfList.get(i);
//           // fieldNames[i] = fieldConf.getName();
//            columnNameList.add(fieldConf.getName());
//            // dynamicPreparedStmt.columnTypeList.add(fieldConf.getType());
//        }

        Optional<Pair<String, List<String>>> sql = prepareTemplatesSQL(schemaName, tableName, rowKind, dynamicPreparedStmt, jdbcConf.getFullColumn());

        if (!sql.isPresent()) {
            return Optional.empty();
        }
        Pair<String, List<String>> s = sql.get();
        List<String> cols = s.getRight();

        dynamicPreparedStmt.columnNameList = cols;
        dynamicPreparedStmt.fieldNamedPreparedStatement =
                FieldNamedPreparedStatementImpl.prepareStatement(connection, s.getKey(), cols.toArray(new String[cols.size()]));
        return Optional.of(dynamicPreparedStmt);
    }

    /**
     * @return key: SQL , val: colList
     */
    private static Optional<Pair<String, List<String>>> prepareTemplatesSQL(
            String schemaName, String tableName, RowKind rowKind, DynamicPreparedStmt dynamicPreparedStmt, List<String> fullCols) {
        Optional<Pair<String, List<String>>> sql = dynamicPreparedStmt.prepareTemplates(rowKind, schemaName, tableName, fullCols);
        if (LOG.isDebugEnabled()) {
            LOG.debug("db:{},tab:{},sql:{}", schemaName, tableName, sql.isPresent() ? sql.get().getKey() : "none");
        }
        return sql;
    }

    public static DynamicPreparedStmt buildStmt(
            JdbcConf jdbcConf,
            JdbcDialect jdbcDialect,
            List<FieldConf> fieldConfList,
            AbstractRowConverter<?, ?, ?, ?> rowConverter,
            FieldNamedPreparedStatement fieldNamedPreparedStatement) {

        DynamicPreparedStmt dynamicPreparedStmt = new DynamicPreparedStmt(jdbcConf);
        dynamicPreparedStmt.jdbcDialect = jdbcDialect;
        dynamicPreparedStmt.rowConverter = rowConverter;
        dynamicPreparedStmt.fieldNamedPreparedStatement = fieldNamedPreparedStatement;
        List<String> fullCols = new ArrayList<>();
        for (int i = 0; i < fieldConfList.size(); i++) {
            FieldConf fieldConf = fieldConfList.get(i);
            fullCols.add(fieldConf.getName());
            // dynamicPreparedStmt.columnTypeList.add(fieldConf.getType());
        }

        dynamicPreparedStmt.columnNameList = fullCols;
        return dynamicPreparedStmt;
    }

    private String createInsertIntoStatement(String schemaName, String tableName, List<String> fullCols) {
        return jdbcDialect.getInsertIntoStatement(schemaName, tableName, fullCols);
    }

    protected Optional<Pair<String, List<String>>> prepareTemplates(
            RowKind rowKind, String schemaName, String tableName, List<String> fullCols) {
        if (CollectionUtils.isEmpty(fullCols)) {
            throw new IllegalArgumentException("fullCols can not be empty");
        }
        String singleSql = null;
        switch (rowKind) {
            case INSERT:
            {
                singleSql = createInsertIntoStatement(schemaName, tableName, fullCols);
                return Optional.of(Pair.of(singleSql, fullCols));
            }
            case UPDATE_AFTER: {
                Optional<String> replaceSql = jdbcDialect.getReplaceStatement(schemaName, tableName, fullCols);
                return replaceSql.isPresent()
                        ? Optional.of(Pair.of(replaceSql.get(), fullCols))
                        : Optional.of(Pair.of(createInsertIntoStatement(schemaName, tableName, fullCols), fullCols));
            }
            case DELETE:
                singleSql = jdbcDialect.getDeleteStatement(schemaName, tableName, this.jdbcConf.getUniqueKey());
                return Optional.of(Pair.of(singleSql, this.jdbcConf.getUniqueKey()));
            case UPDATE_BEFORE: {
                Optional<String> updateBeforeStatement = jdbcDialect.getUpdateBeforeStatement(schemaName, tableName, fullCols);
                return updateBeforeStatement.isPresent()
                        ? Optional.of(Pair.of(updateBeforeStatement.get(), fullCols))
                        : Optional.empty();
            }
            default: {
                // TODO 异常如何处理
                //LOG.warn("not support RowKind: {}", rowKind);
                throw new IllegalArgumentException("not support RowKind: " + rowKind);
            }
        }

        //  return singleSql;
    }

    public void getColumnNameList(Map<String, Integer> header, Set<String> extHeader) {
//        if (writeExtInfo) {
//            columnNameList.addAll(header.keySet());
//        } else {
//            header.keySet().stream()
//                    .filter(fieldName -> !extHeader.contains(fieldName))
//                    .forEach(fieldName -> columnNameList.add(fieldName));
//        }
        throw new UnsupportedOperationException();
    }

    public void buildRowConvert() {
//        RowType rowType =
//                TableUtil.createRowTypeByColsMeta(
//                        columnNameList, columnTypeList, jdbcDialect.getRawTypeConverter());
//        rowConverter = jdbcDialect.getColumnConverter(rowType, jdbcConf);
        throw new UnsupportedOperationException();
    }

    public void getColumnMeta(String schema, String table, Connection dbConn) {
//        Pair<List<String>, List<String>> listListPair =
//                JdbcUtil.getTableMetaData(null, schema, table, dbConn);
//        List<String> nameList = listListPair.getLeft();
//        List<String> typeList = listListPair.getRight();
//        for (String columnName : columnNameList) {
//            int index = nameList.indexOf(columnName);
//            columnTypeList.add(typeList.get(index));
//        }
        throw new UnsupportedOperationException();
    }

    public void close() throws SQLException {
        fieldNamedPreparedStatement.close();
    }

    public FieldNamedPreparedStatement getFieldNamedPreparedStatement() {
        return fieldNamedPreparedStatement;
    }

    public void setFieldNamedPreparedStatement(
            FieldNamedPreparedStatement fieldNamedPreparedStatement) {
        this.fieldNamedPreparedStatement = fieldNamedPreparedStatement;
    }

    public AbstractRowConverter getRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }
}
