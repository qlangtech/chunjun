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

package com.dtstack.chunjun.ddl.convent.mysql.parse;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlAlterLogFileGroup extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER LOGFILE GROUP", SqlKind.ALTER_TABLE);

    private SqlIdentifier logfileGroup;
    private SqlNode fileNmae;
    private SqlNode size;
    private SqlLiteral engineName;

    public SqlAlterLogFileGroup(
            SqlParserPos pos,
            SqlIdentifier logfileGroup,
            SqlNode fileNmae,
            SqlNode size,
            SqlLiteral engineName) {
        super(pos);
        this.logfileGroup = logfileGroup;
        this.fileNmae = fileNmae;
        this.size = size;
        this.engineName = engineName;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(logfileGroup, fileNmae, size, engineName);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());
        logfileGroup.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ADD");
        writer.keyword("UNDOFILE");
        fileNmae.unparse(writer, leftPrec, rightPrec);
        if (size != null) {
            writer.keyword("INITIAL_SIZE");
            size.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("ENGINE");
        engineName.unparse(writer, leftPrec, rightPrec);
    }
}
