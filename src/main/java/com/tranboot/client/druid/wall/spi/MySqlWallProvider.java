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
package com.tranboot.client.druid.wall.spi;

import com.tranboot.client.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.tranboot.client.druid.sql.dialect.mysql.visitor.MySqlExportParameterVisitor;
import com.tranboot.client.druid.sql.parser.SQLStatementParser;
import com.tranboot.client.druid.sql.visitor.ExportParameterVisitor;
import com.tranboot.client.druid.util.JdbcConstants;
import com.tranboot.client.druid.wall.WallConfig;
import com.tranboot.client.druid.wall.WallProvider;
import com.tranboot.client.druid.wall.WallVisitor;

public class MySqlWallProvider extends WallProvider {

    public final static String DEFAULT_CONFIG_DIR = "META-INF/druid/wall/mysql";

    public MySqlWallProvider(){
        this(new WallConfig(DEFAULT_CONFIG_DIR));
    }

    public MySqlWallProvider(WallConfig config){
        super(config, JdbcConstants.MYSQL);
    }

    @Override
    public SQLStatementParser createParser(String sql) {
        return new MySqlStatementParser(sql);
    }

    @Override
    public WallVisitor createWallVisitor() {
        return new MySqlWallVisitor(this);
    }

    @Override
    public ExportParameterVisitor createExportParameterVisitor() {
        return new MySqlExportParameterVisitor();
    }

}
