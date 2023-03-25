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

package com.dtstack.chunjun.connector.mongodb.config;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.ServerAddress;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class MongoClientConfig implements Serializable {

    private static final long serialVersionUID = 8173722133864479906L;

    private String uri;
    private String database;
    private String collection;
    private List<ServerAddress> serverAddresses;
    private String username;
    private String password;
    private String authenticationMechanism = AuthenticationMechanism.SCRAM_SHA_1.getMechanismName();
    private MongodbClientOptions mongodbClientOptions;
}
