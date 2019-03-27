package com.tranboot.client.sqlast;

import java.util.List;

import com.tranboot.client.druid.sql.SQLUtils;
import com.tranboot.client.druid.sql.ast.SQLStatement;
import com.tranboot.client.druid.sql.ast.expr.SQLIdentifierExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLVariantRefExpr;
import com.tranboot.client.druid.sql.ast.statement.SQLInsertStatement;
import com.tranboot.client.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.tranboot.client.druid.sql.builder.impl.SQLBuilderImpl;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.tranboot.client.druid.sql.dialect.oracle.ast.stmt.OracleInsertStatement;
import com.tranboot.client.druid.util.JdbcConstants;

public class SqlInsertStatementBuilderImpl extends SQLBuilderImpl implements SqlInsertStatementBuilder {

	private SQLInsertStatement stmt;
	
	private String dbType;
	
	public SqlInsertStatementBuilderImpl(String dbType){
        this.dbType = dbType;
    }
    
    public SqlInsertStatementBuilderImpl(String sql, String dbType){
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        if (stmtList.size() == 0) {
            throw new IllegalArgumentException("not support empty-statement :" + sql);
        }

        if (stmtList.size() > 1) {
            throw new IllegalArgumentException("not support multi-statement :" + sql);
        }

        SQLInsertStatement stmt = (SQLInsertStatement) stmtList.get(0);
        this.stmt = stmt;
        this.dbType = dbType;
    }

    public SqlInsertStatementBuilderImpl(SQLInsertStatement stmt, String dbType){
        this.stmt = stmt;
        this.dbType = dbType;
    }
	
	@Override
	public SqlInsertStatementBuilder insert(String table) {
		SQLInsertStatement statement = getSQLInsertStatement();
		statement.setTableName(new SQLIdentifierExpr(table));
		statement.setTableSource(new SQLIdentifierExpr(table));
		return this;
	}
	
	
	public SQLInsertStatement getSQLInsertStatement() {
        if (stmt == null) {
        	stmt = createSQLInsertStatement();
        }
        return stmt;
    }
	
	public SQLInsertStatement createSQLInsertStatement() {
        if (JdbcConstants.MYSQL.equals(dbType)) {
            return new MySqlInsertStatement();    
        }
        
        if (JdbcConstants.ORACLE.equals(dbType)) {
            return new OracleInsertStatement();    
        }
        return new SQLInsertStatement();
    }

	@Override
	public SqlInsertStatementBuilder column(String... columns) {
		SQLInsertStatement statement = getSQLInsertStatement();
		if(statement.getColumns() != null && statement.getColumns().size() > 0) {
			throw new IllegalStateException("不能多次调用column");
		}
		for(String column : columns) {
			SQLIdentifierExpr field = new SQLIdentifierExpr(column);
			field.setParent(statement);
			statement.addColumn(field);
		}
		value(columns);
		return this;
	}

	private SqlInsertStatementBuilder value(String... columns) {
		SQLInsertStatement statement = getSQLInsertStatement();
		ValuesClause valuesClause = new ValuesClause();
		valuesClause.setParent(statement);
		for(int i = 0;i < columns.length; i++) {
			SQLVariantRefExpr ref = new SQLVariantRefExpr("?");
			ref.setIndex(i);
			valuesClause.addValue(ref);
		}
		statement.setValues(valuesClause);
		return this;
	}

}
