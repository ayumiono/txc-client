package com.tranboot.client.model.dbsync;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tranboot.client.druid.sql.ast.SQLExpr;
import com.tranboot.client.druid.sql.ast.SQLStatement;
import com.tranboot.client.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLIdentifierExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLInListExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLLiteralExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLPropertyExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLVariantRefExpr;
import com.tranboot.client.druid.sql.ast.statement.SQLExprTableSource;
import com.tranboot.client.druid.sql.ast.statement.SQLInsertStatement;
import com.tranboot.client.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.tranboot.client.druid.sql.ast.statement.SQLUpdateSetItem;
import com.tranboot.client.druid.sql.ast.statement.SQLUpdateStatement;
import com.tranboot.client.druid.sql.visitor.SQLASTVisitorAdapter;
import com.tranboot.client.druid.util.JdbcConstants;
import com.tranboot.client.sqlast.MySQLRewriteVistorAop;
import com.tranboot.client.sqlast.OracleRewriteVistorAop;
import com.tranboot.client.sqlast.SQLASTVisitorAspectAdapter;

/**
 * 
 * 解析结果，用于缓存
 * 需要实现：给定一条sql以及参数数组，映射出最终的同步sql
 * @author xuelong.chen
 *
 */
public class SqlParserProcessor {
	
	SQLStatement statement;
	
	public SqlParserProcessor(SQLStatement statement,SqlParser parser) {
		this.statement = statement;
		this.sqlParser = parser;
	}
	
	/**
	 * 同步表主键值对应下标
	 */
	private Integer targetPrimaryValueIndex;
	private Integer sourcePartitionValueIndex;
	private Integer targetPartitionValueIndex;
	
	private boolean needSplit = false;
	
	/**
	 * 同步表主键值(没有占位符的情况下直接获取值)
	 */
	private Object targetPrimaryValue;
	private Object sourcePartitionValue;
	private Object targetPartitionValue;
	
	/**
	 * 用于更新执行sql 用的object[]参数数组
	 * 
	 * oldField->index->value
	 * f1	   ->0	  -> 10000
	 * newF           ->"00001"
	 * args[0] = "00001"
	 */
	private Map<String, SQLVariantRefExpr> updateItemsIndex;
	private Map<String, SQLVariantRefExpr> conditionIndex;
	
	
	private SqlParser sqlParser;
	
	/**
	 * 解析sql
	 * @param sql
	 * @param args
	 * @return
	 */
	public SqlTransformResult parse() {
		SQLASTVisitorAdapter rewriter = null;
		if(JdbcConstants.MYSQL.equals(dbType())) {
			rewriter = new MySQLRewriteVistorAop(new SQLASTVisitorAspectAdvice());
		}else if(JdbcConstants.ORACLE.equals(dbType())) {
			rewriter = new OracleRewriteVistorAop(new SQLASTVisitorAspectAdvice());
		}else {
			SqlTransformResult result = new SqlTransformResult(statement,sqlParser);
			return result;
		}
		statement.accept(rewriter);
		if(targetPrimaryValue == null && targetPrimaryValueIndex == null) {
			return SqlTransformResult.b;
		}
		SqlTransformResult result = new SqlTransformResult(statement,sqlParser);
		result.setConditionIndex(conditionIndex);
		result.setUpdateItemsIndex(updateItemsIndex);
		result.setTargetPrimaryValueIndex(targetPrimaryValueIndex);
		result.setSourcePartitionValueIndex(sourcePartitionValueIndex);
		result.setTargetPartitionValueIndex(targetPartitionValueIndex);
		result.setTargetPrimaryValue(targetPrimaryValue);
		result.setTargetPartitionValue(targetPartitionValue);
		result.setSourcePartitionValue(sourcePartitionValue);
		result.setNeedSplit(needSplit);
		return result;
	}
	
	/**
	 * 源数据库类型
	 * @return
	 */
	public String dbType() {
		return sqlParser.getMapper().getSourceDbType();
	}
	
	
	class SQLASTVisitorAspectAdvice extends SQLASTVisitorAspectAdapter {
		
		@Override
		public SQLExprTableSource tableAspect(SQLExprTableSource table) {
			SQLIdentifierExpr newTable = new SQLIdentifierExpr(sqlParser.getMapper().getTargetTable());
			table.setExpr(newTable);
			return table;
		}

		@Override
		public SQLUpdateSetItem updateItemAspect(SQLUpdateSetItem updateItem) {
			
			
			if(updateItemsIndex == null) {
				updateItemsIndex = new HashMap<String, SQLVariantRefExpr>();
			}
			String fieldName = null;
			if(updateItem.getColumn() instanceof SQLIdentifierExpr) {
				fieldName = ((SQLIdentifierExpr)updateItem.getColumn()).getName();
			}
			if(updateItem.getColumn() instanceof SQLPropertyExpr) {
				fieldName = ((SQLPropertyExpr)updateItem.getColumn()).getName();
			}
			
			if(fieldName == null) return null;
			String targetField = sqlParser.getMapper().mapField(fieldName);
			if(updateItem.getColumn() instanceof SQLIdentifierExpr) {
				((SQLIdentifierExpr) updateItem.getColumn()).setName(targetField);
			}
			if(updateItem.getColumn() instanceof SQLPropertyExpr) {
				((SQLPropertyExpr) updateItem.getColumn()).setName(targetField);
				//默认是不带UpdateSetItem中的SqlPropertyExpr是不带parent的，这里补上，使得能走到updateColumnAspect
				((SQLPropertyExpr) updateItem.getColumn()).setParent(updateItem);
			}
			if(updateItem.getValue() instanceof SQLBinaryOpExpr) {//lock_qty=lock_qty+?;lock_qty=other_field+?
				SQLBinaryOpExpr value = (SQLBinaryOpExpr) updateItem.getValue();
				if(value.getLeft() instanceof SQLIdentifierExpr) {
					String rightField = ((SQLIdentifierExpr) value.getLeft()).getName();
					((SQLIdentifierExpr) value.getLeft()).setName(sqlParser.getMapper().mapField(rightField));
				}else if(value.getLeft() instanceof SQLPropertyExpr) {
					String rightField = ((SQLPropertyExpr) value.getLeft()).getName();
					((SQLPropertyExpr) value.getLeft()).setName(sqlParser.getMapper().mapField(rightField));
				}
			}
			if(updateItem.getValue() instanceof SQLVariantRefExpr) {
				updateItemsIndex.put(fieldName, (SQLVariantRefExpr)updateItem.getValue());
			}
			return updateItem;
		}

		@Override
		public String insertColumnAspect(String columnName,SQLInsertStatement parent) {
			return sqlParser.getMapper().mapField(columnName);
		}

		@Override
		public String updateColumnAspect(String columnName) {
			//这里只需要做映射的原因是在updateItemAspect方法中已经将整个需要排除字段的UpdateSetItem删除掉了
			return sqlParser.getMapper().mapField(columnName);
		}

		@Override
		public String whereColumnAspect(String columnName,SQLVariantRefExpr right) {
			if(conditionIndex == null) {
				conditionIndex = new HashMap<>();
			}
			conditionIndex.put(columnName, right);
			//如果是主键条件
			//FIXME 暂时不考虑目标主键值不存在原条件的情况
			if(columnName.toLowerCase().equals(sqlParser.getMapper().getSourceKeyField())) {
				targetPrimaryValueIndex = right.getIndex();
			}
			if(columnName.toLowerCase().equals(sqlParser.getRouter().getSourcePartitionKey())) {
				sourcePartitionValueIndex = right.getIndex();
			}
			if(columnName.toLowerCase().equals(sqlParser.getRouter().getTargetPartitionKey())) {
				targetPartitionValueIndex = right.getIndex();
			}
			return sqlParser.getMapper().mapField(columnName);
		}

		@Override
		public void insertEnterPoint(SQLInsertStatement insertStatement) {
			// 在这里做insert column过滤操作 因为druid内部是用for来做处理，如果在insertColumnAspect中删除会出现indexout错误
			List<SQLExpr> columns = insertStatement.getColumns();
			Iterator<SQLExpr> columnIterator = columns.iterator();
			int i=0;
			while(columnIterator.hasNext()) {
				SQLIdentifierExpr _column = (SQLIdentifierExpr) columnIterator.next();
				//如果是需要排除的字段，则删除该SQLIdentifierExpr,并连带ValuesClause中的项也删除
				if(sqlParser.getMapper().needExclude(_column.getName())) {
					columnIterator.remove();
					ValuesClause values = insertStatement.getValues();
					values.getValues().remove(i);
				}else {
					if(_column.getName().toLowerCase().equals(sqlParser.getMapper().getSourceKeyField())) {
						ValuesClause values = insertStatement.getValues();
						SQLExpr value = values.getValues().get(i);
						if(value instanceof SQLVariantRefExpr) {
							targetPrimaryValueIndex = ((SQLVariantRefExpr) value).getIndex();
						}else if(value instanceof SQLLiteralExpr){
							targetPrimaryValue = (SQLLiteralExpr) value;
						}else {
							targetPrimaryValueIndex = i;
						}
					}
					if(_column.getName().toLowerCase().equals(sqlParser.getRouter().getSourcePartitionKey())) {
						ValuesClause values = insertStatement.getValues();
						SQLExpr value = values.getValues().get(i);
						if(value instanceof SQLVariantRefExpr) {
							sourcePartitionValueIndex = ((SQLVariantRefExpr) value).getIndex();
						}else if(value instanceof SQLLiteralExpr){
							sourcePartitionValue = (SQLLiteralExpr) value;
						}else {
							sourcePartitionValueIndex = i;
						}
					}
					if(_column.getName().toLowerCase().equals(sqlParser.getRouter().getTargetPartitionKey())) {
						ValuesClause values = insertStatement.getValues();
						SQLExpr value = values.getValues().get(i);
						if(value instanceof SQLVariantRefExpr) {
							targetPartitionValueIndex = ((SQLVariantRefExpr) value).getIndex();
						}else if(value instanceof SQLLiteralExpr){
							targetPartitionValue = (SQLLiteralExpr) value;
						}else {
							targetPartitionValueIndex = i;
						}
					}
				}
				i++;
			}
		}

		@Override
		public void updateEnterPoint(SQLUpdateStatement updateStatement) {
			List<SQLUpdateSetItem> setItems = updateStatement.getItems();
			Iterator<SQLUpdateSetItem> iterator = setItems.iterator();
			while(iterator.hasNext()) {
				SQLUpdateSetItem item = iterator.next();
				String fieldName = null;
				if(item.getColumn() instanceof SQLIdentifierExpr) {
					fieldName = ((SQLIdentifierExpr)item.getColumn()).getName();
				}
				if(item.getColumn() instanceof SQLPropertyExpr) {
					fieldName = ((SQLPropertyExpr)item.getColumn()).getName();
				}
				if(sqlParser.getMapper().needExclude(fieldName)) {
					//需要排除当前字段
					iterator.remove();
				}
			}
		}

		@Override
		public String whereColumnAspect(String columnName, SQLLiteralExpr right) {
			if(columnName.toLowerCase().equals(sqlParser.getMapper().getSourceKeyField())) {
				targetPrimaryValue = right;
			}
			if(columnName.toLowerCase().equals(sqlParser.getRouter().getSourcePartitionKey())) {
				sourcePartitionValue = right;
			}
			if(columnName.toLowerCase().equals(sqlParser.getRouter().getTargetPartitionKey())) {
				targetPartitionValue = right;
			}
			return sqlParser.getMapper().mapField(columnName);
		}
		
		/* (non-Javadoc)
		 * @see com.gb.dbsync.client.visitor.SQLASTVisitorAspect#whereColumnAspect(java.lang.String, com.alibaba.druid.sql.ast.expr.SQLInListExpr)
		 * 目前只针对update语句中存在in
		 */
		@Override
		public String whereColumnAspect(String columnName, SQLInListExpr in) {
			if(columnName.toLowerCase().equals(sqlParser.getMapper().getTargetKeyField())) {//只处理主键
				needSplit = true;
				List<SQLExpr> exprs = in.getTargetList();
				if(exprs.size() == 1 && exprs.get(0) instanceof SQLVariantRefExpr) {//column in (?)
					targetPrimaryValueIndex = ((SQLVariantRefExpr) exprs.get(0)).getIndex();
				}else {
					targetPrimaryValue = exprs;
				}
			}
//			if(columnName.toLowerCase().equals(sqlParser.getMapper().getTargetKeyField())) {//只处理主键
//				if(in.getParent() instanceof SQLBinaryOpExpr) {
//					SQLBinaryOpExpr parent = (SQLBinaryOpExpr) in.getParent();
//					List<SQLExpr> exprs = in.getTargetList();
//					SQLVariantRefExpr newv = null;
//					if(exprs.size() == 1 && exprs.get(0) instanceof SQLVariantRefExpr) {//column in (?)
//						targetPrimaryValueIndex = ((SQLVariantRefExpr) exprs.get(0)).getIndex();
//						newv = (SQLVariantRefExpr) exprs.get(0);
//					} else {
//						throw new RuntimeException("双写业务中，只支持in条件为占位符");
//					}
//					
//					SQLBinaryOpExpr branch = new SQLBinaryOpExpr();//FIXME
//					in.getExpr().setParent(branch);
//					newv.setParent(branch);
//					branch.setLeft(in.getExpr());
//					branch.setOperator(SQLBinaryOperator.Equality);
//					branch.setRight(newv);
//					if(side == 0) {//left
//						parent.setLeft(branch);
//					}else {//right
//						parent.setRight(branch);
//					}
//					branch.setParent(in.getParent());
//				}else if(in.getParent() instanceof SQLUpdateStatement){
//					SQLVariantRefExpr newv = null;
//					List<SQLExpr> exprs = in.getTargetList();
//					if(exprs.size() == 1 && exprs.get(0) instanceof SQLVariantRefExpr) {//column in (?)
//						targetPrimaryValueIndex = ((SQLVariantRefExpr) exprs.get(0)).getIndex();
//						newv = (SQLVariantRefExpr) exprs.get(0);
//					} else {
//						throw new RuntimeException("双写业务中，只支持in条件为占位符");
//					}
//					SQLUpdateStatement parent = (SQLUpdateStatement) in.getParent();
//					SQLBinaryOpExpr branch = new SQLBinaryOpExpr();//FIXME
//					in.getExpr().setParent(branch);
//					branch.setLeft(in.getExpr());
//					branch.setOperator(SQLBinaryOperator.Equality);
//					branch.setRight(newv);
//					branch.setParent(in.getParent());
//					parent.setWhere(branch);
//				}
//			}
			return sqlParser.getMapper().mapField(columnName);
		}
	}
}
