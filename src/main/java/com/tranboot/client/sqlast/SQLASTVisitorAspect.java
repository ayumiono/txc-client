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

public interface SQLASTVisitorAspect {
	
	/**
	 * 在这里做表映射
	 */
	public SQLExprTableSource tableAspect(SQLExprTableSource table);

	/**
	 * update 项目，包括列和值
	 * @param updateItem
	 * @return
	 */
	public SQLUpdateSetItem updateItemAspect(SQLUpdateSetItem updateItem);

	/**
	 * insert 字段
	 * 在这里做排除字段及映射字段操作
	 * @param columnName
	 * @return
	 */
	public String insertColumnAspect(String columnName, SQLInsertStatement parent);

	/**
	 * update 字段
	 * 在这里做映射字段操作
	 * @param columnName
	 * @return
	 */
	public String updateColumnAspect(String columnName);

	/**
	 * where 字段
	 * 在这里做映射字段操作 只处理形如column SqlBinaryOperator ?
	 * @param columnName
	 * @return
	 */
	public String whereColumnAspect(String columnName, SQLVariantRefExpr right);
	
	/**
	 * where 字段
	 * 在这里做映射字段操作 处理形如column=11111
	 * @param columnName
	 * @return
	 */
	public String whereColumnAspect(String columnName, SQLLiteralExpr right);
	
	public void whereBinaryOpAspect(SQLVariantRefExpr right);
	
	/**
	 * where字段
	 * column in (?);column in (1,2,3)
	 * @param columnName
	 * @param in
	 */
	public String whereColumnAspect(String columnName,SQLInListExpr in);
	
	/**
	 * where入口
	 * @param updateStatement
	 */
	public abstract void whereEnterPoint(SQLBinaryOpExpr x);
	
	public abstract void whereExitPoint(SQLBinaryOpExpr x);

	/**
	 * insert 入口
	 * @param insertStatement
	 */
	public abstract void insertEnterPoint(SQLInsertStatement insertStatement);

	/**
	 * upate 入口
	 * @param updateStatement
	 */
	public abstract void updateEnterPoint(SQLUpdateStatement updateStatement);
	
	public abstract void deleteEnterPoint(SQLDeleteStatement deleteStatement);
	
	public abstract void variantRefAspect(SQLVariantRefExpr x); 
}
