package com.tranboot.client.model.txc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.tranboot.client.druid.sql.SQLUtils;
import com.tranboot.client.druid.sql.ast.SQLExpr;
import com.tranboot.client.druid.sql.ast.SQLStatement;
import com.tranboot.client.druid.sql.ast.expr.SQLIdentifierExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLPropertyExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLVariantRefExpr;
import com.tranboot.client.druid.sql.ast.statement.SQLDeleteStatement;
import com.tranboot.client.druid.sql.ast.statement.SQLInsertStatement;
import com.tranboot.client.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.tranboot.client.druid.sql.ast.statement.SQLUpdateSetItem;
import com.tranboot.client.druid.sql.ast.statement.SQLUpdateStatement;
import com.tranboot.client.druid.util.JdbcUtils;
import com.tranboot.client.model.SQLType;
import com.tranboot.client.sqlast.MySQLRewriteVistorAop;
import com.tranboot.client.sqlast.SQLASTVisitorAspectAdapter;

/**
 * 增加txc字段
 * 
 * @author xuelong.chen since 4.0.0 
 * thread unsafe
 */
public class TxcSQLTransformer {

	public static final String TXC_FILED = "txc";
	
	private TxcSQLTransform result;
	
	public TxcSQLTransformer() {
		result = new TxcSQLTransform();
	}
	
	/**
	 * 需要解析两次，第一次插入txc,第二步解析出txc参数的下标
	 * @param originalSql
	 * @return
	 */
	public TxcSQLTransform transform(String originalSql) throws Exception {
		try {
			SQLStatement statement = SQLUtils.parseStatements(cleanSql(originalSql), JdbcUtils.MYSQL).get(0);
			StringBuilder sbuilder = new StringBuilder();
			SQLTransformHandler handler = new SQLTransformHandler();
			statement.accept(new MySQLRewriteVistorAop(null,sbuilder,handler));
			handler.firstStepFinish();
			String sql = sbuilder.toString();
			result.sql = sql;
			sbuilder = new StringBuilder();
			statement = SQLUtils.parseStatements(sql, JdbcUtils.MYSQL).get(0);
			statement.accept(new MySQLRewriteVistorAop(null,sbuilder,handler));
			return result;
		} catch (Exception e) {
			throw e;
		}
	}

	class SQLTransformHandler extends SQLASTVisitorAspectAdapter {
		
		boolean firstStepFinish = false;
		
		public void firstStepFinish() {
			this.firstStepFinish = true;
		}
		
		@Override
		public void updateEnterPoint(SQLUpdateStatement updateStatement) {
			result.sqlType = SQLType.UPDATE;
			if(firstStepFinish) {
				result.additionIndex = ((SQLVariantRefExpr)updateStatement.getItems().get(updateStatement.getItems().size()-1).getValue()).getIndex();
				return;
			}
			String alias = updateStatement.getTableSource().getAlias();
			SQLUpdateSetItem txcUpd = new SQLUpdateSetItem();
			txcUpd.setParent(updateStatement);
			if (alias == null) {
				txcUpd.setColumn(new SQLIdentifierExpr(TXC_FILED));
			} else {
				SQLPropertyExpr property = new SQLPropertyExpr();
				SQLIdentifierExpr owner = new SQLIdentifierExpr(alias);
				owner.setParent(property);
				property.setOwner(owner);
				property.setName(TXC_FILED);
				txcUpd.setColumn(property);
			}
			SQLVariantRefExpr v = new SQLVariantRefExpr("?");
			txcUpd.setValue(v);
			updateStatement.getItems().add(txcUpd);
		}

		@Override
		public void insertEnterPoint(SQLInsertStatement insertStatement) {
			result.sqlType = SQLType.INSERT;
			if(firstStepFinish) {
				result.additionIndex = ((SQLVariantRefExpr)insertStatement.getValues().getValues().get(insertStatement.getValues().getValues().size()-1)).getIndex();
				return;
			}
			List<SQLExpr> columns = insertStatement.getColumns();
			ValuesClause values = insertStatement.getValues();
			List<SQLExpr> _values = values.getValues();
			SQLIdentifierExpr txcField = new SQLIdentifierExpr(TXC_FILED);
			columns.add(txcField);
			_values.add(new SQLVariantRefExpr("?"));
		}

		@Override
		public void deleteEnterPoint(SQLDeleteStatement deleteStatement) {
			// do nothing
			result.sqlType = SQLType.DELETE;
		}
	}
	
	public static class TxcSQLTransform {
		
		public String sql;
		public int additionIndex;
		public SQLType sqlType;
		
		public Object[] addArgs(Object[] args, String txc) {
			List<Object> aggs = new ArrayList<>();
			for(int i=0;i<additionIndex;i++) {
				aggs.add(args[i]);
			}
			aggs.add(txc);
			for(int i=additionIndex;i<args.length;i++) {
				aggs.add(args[i]);
			}
			return aggs.toArray(new Object[] {});
		}
		
		/**
		 * 针对batchUpdate(java.lang.String...)场景下的特殊处理,由于不能传参数，所以必须把txc参数解析到sql中去
		 * @param sql
		 * @param txc
		 * @return
		 */
		public static String process4BatchUpd(String sql,String txc) {
			SQLStatement statement = SQLUtils.parseStatements(sql, JdbcUtils.MYSQL).get(0);
			StringBuilder sbuilder = new StringBuilder();
			statement.accept(new MySQLRewriteVistorAop(Collections.singletonList(txc),sbuilder,new SQLASTVisitorAspectAdapter()));
			return sbuilder.toString();
		}
	}
	
	protected String cleanSql(String sql) {
		//清洗掉mycat中的注解   /*!mycat:db_type=master*/update .....
		if(sql.indexOf("mycat")>-1) {
			return StringUtils.substringAfterLast(sql, "*/");
		}
		return sql;
	}
}
