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

package com.dtstack.chunjun.connector.clickhouse.source;

import com.dtstack.chunjun.connector.clickhouse.util.ClickhouseUtil;
import com.dtstack.chunjun.connector.jdbc.TableCols;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputSplit;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @program chunjun
 * @author: xiuzhu
 * @create: 2021/05/10
 */
public class ClickhouseInputFormat extends JdbcInputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void openInternal(InputSplit inputSplit) {
        JdbcInputSplit jdbcInputSplit = (JdbcInputSplit) inputSplit;
        initMetric(jdbcInputSplit);
        if (!canReadData(jdbcInputSplit)) {
            LOG.warn(
                    "Not read data when the start location are equal to end location, start = {}, end = {}",
                    jdbcInputSplit.getStartLocation(),
                    jdbcInputSplit.getEndLocation());
            hasNext = false;
            return;
        }

        String querySQL = null;
        try {
            dbConn = getConnection();

            // TableCols pair = null;
//            List<String> fullColumnList = new LinkedList<>();
//            List<String> fullColumnTypeList = new LinkedList<>();
            // if (StringUtils.isBlank(jdbcConf.getCustomSql())) {
            //  pair = getTableMetaData();
//                fullColumnList = pair.getLeft();
//                fullColumnTypeList = pair.getRight();
            //}
//            List<ColMeta> columnPair =
//                    ColumnBuildUtil.handleColumnList(
//                            jdbcConf.getColumn(), pair);
//            columnNameList = columnPair.getLeft();
//            columnTypeList = columnPair.getRight();

            querySQL = buildQuerySql(jdbcInputSplit);
            jdbcConf.setQuerySql(querySQL);
            executeQuery(jdbcInputSplit.getStartLocation());
            if (!resultSet.isClosed()) {
                columnCount = resultSet.getMetaData().getColumnCount();
            }
            // 增量任务
            needUpdateEndLocation =
                    jdbcConf.isIncrement() && !jdbcConf.isPolling() && !jdbcConf.isUseMaxFunc();

            TableCols tableCols = new TableCols(this.colsMeta);
            RowType rowType =
                    TableUtil.createRowType(
                            tableCols.filterBy(jdbcConf.getColumn()), jdbcDialect.getRawTypeConverter());
            setRowConverter(
                    rowConverter == null
                            ? jdbcDialect.getColumnConverter(rowType, jdbcConf)
                            : rowConverter);
        } catch (SQLException se) {
            String expMsg = se.getMessage();
            expMsg = querySQL == null ? expMsg : expMsg + "\n querySQL: " + querySQL;
            throw new IllegalArgumentException("open() failed." + expMsg, se);
        }
    }

    @Override
    protected Connection getConnection() throws SQLException {
        return ClickhouseUtil.getConnection(
                jdbcConf.getJdbcUrl(), jdbcConf.getUsername(), jdbcConf.getPassword());
    }
}
