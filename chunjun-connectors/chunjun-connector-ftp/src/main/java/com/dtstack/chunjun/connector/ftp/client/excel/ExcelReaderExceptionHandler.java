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

package com.dtstack.chunjun.connector.ftp.client.excel;

import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author by dujie @Description @Date 2021/12/20 */
public class ExcelReaderExceptionHandler implements Thread.UncaughtExceptionHandler {

  //  protected final Logger LOG = LoggerFactory.getLogger(getClass());
    protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOG.error(
                "an error occurred during the reading of the Excel file."
                        + " thread name : {}, message : {}",
                t.getName(),
                e.getMessage());
    }
}
