package com.tranboot.client.model.dbsync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.tranboot.client.druid.sql.ast.SQLStatement;
import com.tranboot.client.druid.sql.ast.expr.SQLVariantRefExpr;
import com.tranboot.client.druid.sql.ast.statement.SQLExprTableSource;
import com.tranboot.client.druid.util.JdbcConstants;
import com.tranboot.client.sqlast.MySQLRewriteVistorAop;
import com.tranboot.client.sqlast.OracleRewriteVistorAop;
import com.tranboot.client.sqlast.SQLASTVisitorAspectAdapter;

/**
 * 
 * 数据双写用
 * @author xuelong.chen
 *
 */
public class SqlTransformResult {
	
	public static final SqlTransformResult a = new SqlTransformResult(null, null);//no need to dubble write
	public static final SqlTransformResult b = new SqlTransformResult(null, null);//no primary key
	
	private final SQLStatement statement;
	private final SqlParser sqlparser;
	
	/**
	 * 同步表主键值对应下标
	 */
	private Integer targetPrimaryValueIndex;
	private Integer sourcePartitionValueIndex;
	private Integer targetPartitionValueIndex;
	private Object targetPrimaryValue;
	private Object sourcePartitionValue;
	private Object targetPartitionValue;
	
	private boolean needSplit = false;
	
	private Map<String, SQLVariantRefExpr> updateItemsIndex;
	private Map<String, SQLVariantRefExpr> conditionIndex;
	
	public SqlTransformResult(SQLStatement statement,SqlParser sqlparser) {
		this.statement = statement;
		this.sqlparser = sqlparser;
	}
	
	@SuppressWarnings("unchecked")
	public String[] primaryKeyValue(Object[] args) {
		try {
			if(!needSplit) {
				if(targetPrimaryValue != null) {
					return new String[] {targetPrimaryValue.toString()};
				}
				
				if(targetPrimaryValueIndex == null) {
					throw new RuntimeException("没有业务主键，无法进行数据双写操作");
				} 
				
				Object keyValue = args[targetPrimaryValueIndex];
				return new String[] {keyValue.toString()};
			}else {
				if(targetPrimaryValue != null) {
					List<Object> values  = (List<Object>) targetPrimaryValue;
					String[] d = new String[values.size()];
					for(int i = 0;i<values.size();i++) {
						d[i] = values.get(i).toString();
					}
					return d;
				}
				if(targetPrimaryValueIndex == null) return null;
				return args[targetPrimaryValueIndex].toString().split(",");
			}
		} catch (Exception e) {
			throw e;
		}
	}
	
	public String sourcePartitionValue(Object[] args) {
		try {
			if(sourcePartitionValue != null) return sourcePartitionValue.toString();
			return args[sourcePartitionValueIndex].toString();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String targetPartitionValue(Object[] args) {
		try {
			if(targetPartitionValue != null) return targetPartitionValue.toString();
			return args[targetPartitionValueIndex].toString();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
//	/**
//	 * 目标表
//	 * @param args
//	 * @return
//	 */
//	public String route(Object[] args) {
//		SqlRouter router = sqlparser.getRouter();
//		if(router != null) {
//			String suffix = router.route(primaryKeyValue(args));
//			return getTargetTable() + suffix;
//		}
//		return getTargetTable();
//	}
	
	/**
	 * 输出带参数的sql语句
	 * @param args
	 * @return
	 */
	public String sql(Object[] args) {
		StringBuilder sbuilder = new StringBuilder();
		if(JdbcConstants.MYSQL.equals(getTargetDbType())) {
			MySQLRewriteVistorAop visitor = new MySQLRewriteVistorAop(args == null ? new ArrayList<Object>() : Arrays.asList(args), sbuilder, new SQLASTVisitorAspectAdapter() {
				@Override
				public SQLExprTableSource tableAspect(SQLExprTableSource table) {
					//分表路由
//					SQLIdentifierExpr newTable = new SQLIdentifierExpr(route(args));
//					table.setExpr(newTable);
					return table;
				}
			});
			statement.accept(visitor);
		}else if(JdbcConstants.ORACLE.equals(getTargetDbType())) {
			OracleRewriteVistorAop visitor = new OracleRewriteVistorAop(args == null ? new ArrayList<Object>() : Arrays.asList(args), sbuilder, new SQLASTVisitorAspectAdapter() {
				@Override
				public SQLExprTableSource tableAspect(SQLExprTableSource table) {
					//分表路由
//					SQLIdentifierExpr newTable = new SQLIdentifierExpr(route(args));
//					table.setExpr(newTable);
					return table;
				}
			});
			statement.accept(visitor);
		}
		return sbuilder.toString();
	}
	
	public Map<String, SQLVariantRefExpr> getUpdateItemsIndex() {
		return updateItemsIndex;
	}

	public void setUpdateItemsIndex(Map<String, SQLVariantRefExpr> updateItemsIndex) {
		this.updateItemsIndex = updateItemsIndex;
	}

	public Map<String, SQLVariantRefExpr> getConditionIndex() {
		return conditionIndex;
	}

	public void setConditionIndex(Map<String, SQLVariantRefExpr> conditionIndex) {
		this.conditionIndex = conditionIndex;
	}

	public SQLStatement getStatement() {
		return statement;
	}

	public String getPrimaryKey() {
		return sqlparser.getMapper().getTargetKeyField();
	}
	
	public String getSourceTable() {
		return sqlparser.getMapper().getSourceTable();
	}

	public String getTargetTable() {
		return sqlparser.getMapper().getTargetTable();
	}

	public String getTargetDbType() {
		return sqlparser.getMapper().getTargetDbType();
	}
	
	public String getTargetDb() {
		return sqlparser.getMapper().getTargetDb();
	}
	
	public String getSourceDb() {
		return sqlparser.getMapper().getSourceDb();
	}
	
	public String getSourceDbType() {
		return sqlparser.getMapper().getSourceDbType();
	}
	
	public String getTargetPartitionKey() {
		return sqlparser.getRouter().getTargetPartitionKey();
	}
	
	public String getSourcePartitionKey() {
		return sqlparser.getRouter().getSourcePartitionKey();
	}
	
	public void setTargetPrimaryValueIndex(Integer targetPrimaryValueIndex) {
		this.targetPrimaryValueIndex = targetPrimaryValueIndex;
	}

	public void setSourcePartitionValueIndex(Integer sourcePartitionValueIndex) {
		this.sourcePartitionValueIndex = sourcePartitionValueIndex;
	}

	public void setTargetPartitionValueIndex(Integer targetPartitionValueIndex) {
		this.targetPartitionValueIndex = targetPartitionValueIndex;
	}

	public void setTargetPrimaryValue(Object targetPrimaryValue) {
		this.targetPrimaryValue = targetPrimaryValue;
	}

	public void setSourcePartitionValue(Object sourcePartitionValue) {
		this.sourcePartitionValue = sourcePartitionValue;
	}

	public void setTargetPartitionValue(Object targetPartitionValue) {
		this.targetPartitionValue = targetPartitionValue;
	}

	public boolean isNeedSplit() {
		return needSplit;
	}

	public void setNeedSplit(boolean needSplit) {
		this.needSplit = needSplit;
	}
}
