package com.tranboot.client.sqlast;

import com.tranboot.client.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLInListExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLLiteralExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLVariantRefExpr;
import com.tranboot.client.druid.sql.ast.statement.SQLDeleteStatement;
import com.tranboot.client.druid.sql.ast.statement.SQLExprTableSource;
import com.tranboot.client.druid.sql.ast.statement.SQLInsertStatement;
import com.tranboot.client.druid.sql.ast.statement.SQLUpdateSetItem;
import com.tranboot.client.druid.sql.ast.statement.SQLUpdateStatement;

public class SQLASTVisitorAspectAdapter implements SQLASTVisitorAspect {

	@Override
	public SQLExprTableSource tableAspect(SQLExprTableSource table) {
		return table;
	}

	@Override
	public SQLUpdateSetItem updateItemAspect(SQLUpdateSetItem updateItem) {
		return updateItem;
	}

	@Override
	public String insertColumnAspect(String columnName, SQLInsertStatement parent) {
		return columnName;
	}

	@Override
	public String updateColumnAspect(String columnName) {
		return columnName;
	}

	@Override
	public String whereColumnAspect(String columnName, SQLVariantRefExpr right) {
		return columnName;
	}

	@Override
	public void insertEnterPoint(SQLInsertStatement insertStatement) {

	}

	@Override
	public void updateEnterPoint(SQLUpdateStatement updateStatement) {

	}

	@Override
	public String whereColumnAspect(String columnName, SQLLiteralExpr right) {
		return columnName;
	}

	@Override
	public void whereEnterPoint(SQLBinaryOpExpr x) {
	}
	
	@Override
	public void deleteEnterPoint(SQLDeleteStatement deleteStatement) {
		
	}

	@Override
	public String whereColumnAspect(String columnName, SQLInListExpr in) {
		return columnName;
	}

	@Override
	public void whereBinaryOpAspect(SQLVariantRefExpr right) {
		
	}

	@Override
	public void whereExitPoint(SQLBinaryOpExpr x) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void variantRefAspect(SQLVariantRefExpr x) {
		// TODO Auto-generated method stub
	}
}
