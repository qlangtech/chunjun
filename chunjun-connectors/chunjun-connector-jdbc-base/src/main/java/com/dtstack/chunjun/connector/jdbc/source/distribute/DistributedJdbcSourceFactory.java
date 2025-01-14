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
package com.dtstack.chunjun.connector.jdbc.source.distribute;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.conf.ConnectionConf;
import com.dtstack.chunjun.connector.jdbc.conf.DataSourceConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.qlangtech.tis.plugin.ds.IColMetaGetter;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2022/01/12 Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class DistributedJdbcSourceFactory extends JdbcSourceFactory {

    protected DistributedJdbcSourceFactory(
            SyncConf syncConf, StreamExecutionEnvironment env, JdbcDialect jdbcDialect
            , List<IColMetaGetter> sourceColsMeta) {
        super(syncConf, env, jdbcDialect, sourceColsMeta);
    }

    protected JdbcInputFormatBuilder getBuilder() {
        DistributedJdbcInputFormatBuilder builder =
                new DistributedJdbcInputFormatBuilder(new DistributedJdbcInputFormat());
        List<ConnectionConf> connectionConfList = jdbcConf.getConnection();
        List<DataSourceConf> dataSourceConfList = new ArrayList<>(connectionConfList.size());
        for (ConnectionConf connectionConf : connectionConfList) {
            String currentUsername =
                    (StringUtils.isNotBlank(connectionConf.getUsername()))
                            ? connectionConf.getUsername()
                            : jdbcConf.getUsername();
            String currentPassword =
                    (StringUtils.isNotBlank(connectionConf.getPassword()))
                            ? connectionConf.getPassword()
                            : jdbcConf.getPassword();

            String schema = connectionConf.getSchema();
            for (String table : connectionConf.getTable()) {
                DataSourceConf dataSourceConf = new DataSourceConf();
                dataSourceConf.setUserName(currentUsername);
                dataSourceConf.setPassword(currentPassword);
                dataSourceConf.setJdbcUrl(connectionConf.obtainJdbcUrl());
                dataSourceConf.setTable(table);
                dataSourceConf.setSchema(schema);

                dataSourceConfList.add(dataSourceConf);
            }
        }
        builder.setSourceList(dataSourceConfList);
        return builder;
    }
}
