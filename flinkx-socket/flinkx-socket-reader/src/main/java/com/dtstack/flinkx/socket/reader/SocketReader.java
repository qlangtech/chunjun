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

package com.dtstack.flinkx.socket.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;

import static com.dtstack.flinkx.socket.constants.SocketCons.DEFAULT_ENCODING;
import static com.dtstack.flinkx.socket.constants.SocketCons.KEY_ADDRESS;
import static com.dtstack.flinkx.socket.constants.SocketCons.KEY_ENCODING;
import static com.dtstack.flinkx.socket.constants.SocketCons.KEY_PARSE;

/** 读取用户传入参数
 *
 * @author by kunni@dtstack.com
 */

public class SocketReader extends BaseDataReader {

    protected String address;

    protected String codeC;

    protected ArrayList<String> columns;

    protected String encoding;

    @SuppressWarnings("unchecked")
    public SocketReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig.ParameterConfig parameter = config.getJob().getContent().get(0).getReader().getParameter();
        address = parameter.getStringVal(KEY_ADDRESS);
        codeC = parameter.getStringVal(KEY_PARSE);
        encoding = parameter.getStringVal(KEY_ENCODING, DEFAULT_ENCODING);
        columns = (ArrayList<String>) parameter.getColumn();
    }

    @Override
    public DataStream<Row> readData() {
        SocketBuilder socketBuilder = new SocketBuilder();
        socketBuilder.setAddress(address);
        socketBuilder.setCodeC(codeC);
        socketBuilder.setColumns(columns);
        socketBuilder.setEncoding(encoding);
        socketBuilder.setDataTransferConfig(dataTransferConfig);
        return createInput(socketBuilder.finish());
    }
}