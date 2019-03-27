/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tranboot.client.druid.sql.parser;

import com.tranboot.client.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.tranboot.client.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.tranboot.client.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.tranboot.client.druid.sql.dialect.oracle.parser.OracleExprParser;
import com.tranboot.client.druid.sql.dialect.oracle.parser.OracleLexer;
import com.tranboot.client.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.tranboot.client.druid.util.JdbcUtils;

public class SQLParserUtils {

    public static SQLStatementParser createSQLStatementParser(String sql, String dbType) {
        if (JdbcUtils.ORACLE.equals(dbType) || JdbcUtils.ALI_ORACLE.equals(dbType)) {
            return new OracleStatementParser(sql);
        }

        if (JdbcUtils.MYSQL.equals(dbType)) {
            return new MySqlStatementParser(sql);
        }

        if (JdbcUtils.MARIADB.equals(dbType)) {
            return new MySqlStatementParser(sql);
        }


        if (JdbcUtils.H2.equals(dbType)) {
            return new MySqlStatementParser(sql);
        }
        
        return new SQLStatementParser(sql, dbType);
    }

    public static SQLExprParser createExprParser(String sql, String dbType) {
        if (JdbcUtils.ORACLE.equals(dbType) || JdbcUtils.ALI_ORACLE.equals(dbType)) {
            return new OracleExprParser(sql);
        }

        if (JdbcUtils.MYSQL.equals(dbType) || //
            JdbcUtils.MARIADB.equals(dbType) || //
            JdbcUtils.H2.equals(dbType)) {
            return new MySqlExprParser(sql);
        }

        return new SQLExprParser(sql);
    }

    public static Lexer createLexer(String sql, String dbType) {
        if (JdbcUtils.ORACLE.equals(dbType) || JdbcUtils.ALI_ORACLE.equals(dbType)) {
            return new OracleLexer(sql);
        }

        if (JdbcUtils.MYSQL.equals(dbType) || //
                JdbcUtils.MARIADB.equals(dbType) || //
                JdbcUtils.H2.equals(dbType)) {
            return new MySqlLexer(sql);
        }
        return new Lexer(sql);
    }
}
