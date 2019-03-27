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
package com.tranboot.client.druid.sql.builder.impl;

import java.util.List;

import com.tranboot.client.druid.sql.SQLUtils;
import com.tranboot.client.druid.sql.ast.SQLExpr;
import com.tranboot.client.druid.sql.ast.SQLLimit;
import com.tranboot.client.druid.sql.ast.SQLOrderBy;
import com.tranboot.client.druid.sql.ast.SQLStatement;
import com.tranboot.client.druid.sql.ast.expr.SQLBinaryOperator;
import com.tranboot.client.druid.sql.ast.expr.SQLIdentifierExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLIntegerExpr;
import com.tranboot.client.druid.sql.ast.statement.SQLExprTableSource;
import com.tranboot.client.druid.sql.ast.statement.SQLSelect;
import com.tranboot.client.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.tranboot.client.druid.sql.ast.statement.SQLSelectItem;
import com.tranboot.client.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.tranboot.client.druid.sql.ast.statement.SQLSelectQuery;
import com.tranboot.client.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.tranboot.client.druid.sql.ast.statement.SQLSelectStatement;
import com.tranboot.client.druid.sql.builder.SQLSelectBuilder;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.tranboot.client.druid.sql.dialect.oracle.ast.stmt.OracleSelect;
import com.tranboot.client.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.tranboot.client.druid.util.JdbcConstants;

public class SQLSelectBuilderImpl implements SQLSelectBuilder {

    private SQLSelectStatement stmt;
    private String             dbType;

    public SQLSelectBuilderImpl(String dbType){
        this(new SQLSelectStatement(), dbType);
    }
    
    public SQLSelectBuilderImpl(String sql, String dbType){
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        if (stmtList.size() == 0) {
            throw new IllegalArgumentException("not support empty-statement :" + sql);
        }

        if (stmtList.size() > 1) {
            throw new IllegalArgumentException("not support multi-statement :" + sql);
        }

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        this.stmt = stmt;
        this.dbType = dbType;
    }

    public SQLSelectBuilderImpl(SQLSelectStatement stmt, String dbType){
        this.stmt = stmt;
        this.dbType = dbType;
    }

    public SQLSelect getSQLSelect() {
        if (stmt.getSelect() == null) {
            stmt.setSelect(createSelect());
        }
        return stmt.getSelect();
    }

    @Override
    public SQLSelectStatement getSQLSelectStatement() {
        return stmt;
    }

    public SQLSelectBuilderImpl select(String... columns) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        for (String column : columns) {
            SQLSelectItem selectItem = SQLUtils.toSelectItem(column, dbType);
            queryBlock.addSelectItem(selectItem);
        }

        return this;
    }

    @Override
    public SQLSelectBuilderImpl selectWithAlias(String column, String alias) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLExpr columnExpr = SQLUtils.toSQLExpr(column, dbType);
        SQLSelectItem selectItem = new SQLSelectItem(columnExpr, alias);
        queryBlock.addSelectItem(selectItem);

        return this;
    }

    @Override
    public SQLSelectBuilderImpl from(String table) {
        return from(table, null);
    }

    @Override
    public SQLSelectBuilderImpl from(String table, String alias) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();
        SQLExprTableSource from = new SQLExprTableSource(new SQLIdentifierExpr(table), alias);
        queryBlock.setFrom(from);

        return this;
    }

    @Override
    public SQLSelectBuilderImpl orderBy(String... columns) {
        SQLSelect select = this.getSQLSelect();

        SQLOrderBy orderBy = select.getOrderBy();
        if (orderBy == null) {
            orderBy = createOrderBy();
            select.setOrderBy(orderBy);
        }

        for (String column : columns) {
            SQLSelectOrderByItem orderByItem = SQLUtils.toOrderByItem(column, dbType);
            orderBy.addItem(orderByItem);
        }

        return this;
    }

    @Override
    public SQLSelectBuilderImpl groupBy(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLSelectGroupByClause groupBy = queryBlock.getGroupBy();
        if (groupBy == null) {
            groupBy = createGroupBy();
            queryBlock.setGroupBy(groupBy);
        }

        SQLExpr exprObj = SQLUtils.toSQLExpr(expr, dbType);
        groupBy.addItem(exprObj);

        return this;
    }

    @Override
    public SQLSelectBuilderImpl having(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLSelectGroupByClause groupBy = queryBlock.getGroupBy();
        if (groupBy == null) {
            groupBy = createGroupBy();
            queryBlock.setGroupBy(groupBy);
        }

        SQLExpr exprObj = SQLUtils.toSQLExpr(expr, dbType);
        groupBy.setHaving(exprObj);

        return this;
    }

    @Override
    public SQLSelectBuilderImpl into(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLExpr exprObj = SQLUtils.toSQLExpr(expr, dbType);
        queryBlock.setInto(exprObj);

        return this;
    }

    @Override
    public SQLSelectBuilderImpl where(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLExpr exprObj = SQLUtils.toSQLExpr(expr, dbType);
        queryBlock.setWhere(exprObj);

        return this;
    }

    @Override
    public SQLSelectBuilderImpl whereAnd(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLExpr exprObj = SQLUtils.toSQLExpr(expr, dbType);
        SQLExpr newCondition = SQLUtils.buildCondition(SQLBinaryOperator.BooleanAnd, exprObj, false,
                                                       queryBlock.getWhere());
        queryBlock.setWhere(newCondition);

        return this;
    }

    @Override
    public SQLSelectBuilderImpl whereOr(String expr) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        SQLExpr exprObj = SQLUtils.toSQLExpr(expr, dbType);
        SQLExpr newCondition = SQLUtils.buildCondition(SQLBinaryOperator.BooleanOr, exprObj, false,
                                                       queryBlock.getWhere());
        queryBlock.setWhere(newCondition);

        return this;
    }

    @Override
    public SQLSelectBuilderImpl limit(int rowCount) {
        return limit(rowCount, 0);
    }

    @Override
    public SQLSelectBuilderImpl limit(int rowCount, int offset) {
        SQLSelectQueryBlock queryBlock = getQueryBlock();

        if (queryBlock instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mySqlQueryBlock = (MySqlSelectQueryBlock) queryBlock;

            SQLLimit limit = new SQLLimit();
            limit.setRowCount(new SQLIntegerExpr(rowCount));
            if (offset > 0) {
                limit.setOffset(new SQLIntegerExpr(offset));
            }

            mySqlQueryBlock.setLimit(limit);

            return this;
        }

        if (queryBlock instanceof OracleSelectQueryBlock) {
            OracleSelectQueryBlock oracleQueryBlock = (OracleSelectQueryBlock) queryBlock;
            if (offset <= 0) {
                SQLExpr rowCountExpr = new SQLIntegerExpr(rowCount);
                SQLExpr newCondition = SQLUtils.buildCondition(SQLBinaryOperator.BooleanAnd, rowCountExpr, false,
                                                               oracleQueryBlock.getWhere());
                queryBlock.setWhere(newCondition);
            } else {
                throw new UnsupportedOperationException("not support offset");
            }

            return this;
        }
        
        throw new UnsupportedOperationException();
    }

    protected SQLSelectQueryBlock getQueryBlock() {
        SQLSelect select = getSQLSelect();
        SQLSelectQuery query = select.getQuery();
        if (query == null) {
            query = createSelectQueryBlock();
            select.setQuery(query);
        }

        if (!(query instanceof SQLSelectQueryBlock)) {
            throw new IllegalStateException("not support from, class : " + query.getClass().getName());
        }

        SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
        return queryBlock;
    }

    protected SQLSelect createSelect() {
        if (JdbcConstants.ORACLE.equals(dbType)) {
            return new OracleSelect();
        }

        return new SQLSelect();
    }

    protected SQLSelectQuery createSelectQueryBlock() {
        if (JdbcConstants.MYSQL.equals(dbType)) {
            return new MySqlSelectQueryBlock();
        }

        if (JdbcConstants.ORACLE.equals(dbType)) {
            return new OracleSelectQueryBlock();
        }

        return new SQLSelectQueryBlock();
    }

    protected SQLOrderBy createOrderBy() {
        return new SQLOrderBy();
    }

    protected SQLSelectGroupByClause createGroupBy() {
        return new SQLSelectGroupByClause();
    }

    public String toString() {
        return SQLUtils.toSQLString(stmt, dbType);
    }
}
