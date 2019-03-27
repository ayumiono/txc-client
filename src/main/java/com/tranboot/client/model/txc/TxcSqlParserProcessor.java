package com.tranboot.client.model.txc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import com.codahale.metrics.Timer;
import com.googlecode.aviator.AviatorEvaluator;
import com.tranboot.client.druid.sql.SQLUtils;
import com.tranboot.client.druid.sql.ast.SQLExpr;
import com.tranboot.client.druid.sql.ast.SQLName;
import com.tranboot.client.druid.sql.ast.SQLStatement;
import com.tranboot.client.druid.sql.ast.expr.SQLAllColumnExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLBinaryOperator;
import com.tranboot.client.druid.sql.ast.expr.SQLIdentifierExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLLiteralExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLPropertyExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLVariantRefExpr;
import com.tranboot.client.druid.sql.ast.statement.SQLDeleteStatement;
import com.tranboot.client.druid.sql.ast.statement.SQLExprTableSource;
import com.tranboot.client.druid.sql.ast.statement.SQLInsertStatement;
import com.tranboot.client.druid.sql.ast.statement.SQLSelectItem;
import com.tranboot.client.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.tranboot.client.druid.sql.ast.statement.SQLUpdateSetItem;
import com.tranboot.client.druid.sql.builder.SQLSelectBuilder;
import com.tranboot.client.druid.sql.builder.impl.SQLDeleteBuilderImpl;
import com.tranboot.client.druid.sql.builder.impl.SQLUpdateBuilderImpl;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.tranboot.client.druid.util.JdbcUtils;
import com.tranboot.client.exception.TxcNotSupportException;
import com.tranboot.client.exception.TxcSqlParseException;
import com.tranboot.client.exception.TxcTransactionException;
import com.tranboot.client.model.SQLType;
import com.tranboot.client.model.TableSchema;
import com.tranboot.client.model.txc.SQLParamExtractorPipeline.KeyValuePair;
import com.tranboot.client.model.txc.SQLParamExtractorPipeline.PrimaryKeyExtractor;
import com.tranboot.client.service.TableSchemaCacheLoader;
import com.tranboot.client.sqlast.MySQLRewriteVistorAop;
import com.tranboot.client.sqlast.SQLASTVisitorAspectAdapter;
import com.tranboot.client.sqlast.SqlInsertStatementBuilderImpl;
import com.tranboot.client.sqlast.UniqColumnSQLSelectBuilderImpl;
import com.tranboot.client.utils.LRUCache;
import static com.tranboot.client.utils.MetricsReporter.*;

/**
 * @author xuelong.chen
 * thread unsafe
 */
public class TxcSqlParserProcessor implements TxcSqlProcessor{
	
	SQLStatement statement;
	SQLUpdateBuilderImpl updateBuilder;
	SQLSelectBuilder queryBuilder;
	SQLDeleteBuilderImpl deleteBuilder;
	SqlInsertStatementBuilderImpl insertStatementBuilder;
	SQLParamExtractorPipeline pipeline;
	String tableName;
	String datasource;
	String dbType;
	
	JdbcTemplate jdbcTemplate;
	
	
	SqlParserTxcSQL result;
	
	
	public SQLParamExtractorPipeline getRenderPipeline() {
		return pipeline;
	}
	
	public TxcSqlParserProcessor(JdbcTemplate jdbcTemplate, SQLStatement statement, String datasource){
		this.jdbcTemplate = jdbcTemplate;
		this.statement = statement;
//		this.dbType = statement.getDbType();FIXME 
		this.dbType = JdbcUtils.MYSQL;
		this.datasource = datasource;
	}
	
	@Override
	public SqlParserTxcSQL parse() {
		Timer.Context context = parserProcessorTimer.time();
		try {
			result = new SqlParserTxcSQL();
			queryBuilder = new UniqColumnSQLSelectBuilderImpl(dbType);
			if (statement instanceof MySqlUpdateStatement) {// update
				updateBuilder = new SQLUpdateBuilderImpl(dbType);
				result.rollbackSql = updateBuilder.getSQLUpdateStatement();
				result.sqlType = SQLType.UPDATE;
			} else if (statement instanceof MySqlInsertStatement) {// insert
				deleteBuilder = new SQLDeleteBuilderImpl(dbType);
				result.rollbackSql = deleteBuilder.getSQLDeleteStatement();
				result.sqlType = SQLType.INSERT;
			} else if(statement instanceof SQLDeleteStatement ){// delete
				insertStatementBuilder = new SqlInsertStatementBuilderImpl(dbType);
				result.rollbackSql = insertStatementBuilder.getSQLInsertStatement();
				result.sqlType = SQLType.DELETE;
			}
			pipeline = new SQLParamExtractorPipeline();
			statement.accept(new MySQLRewriteVistorAop(new TxcSQLASTVisitorAdivce()));
			
			if(updateBuilder != null) {//最后进行sql检查,以防万一
				if(updateBuilder.getSQLUpdateStatement().getWhere() == null) {
					throw new TxcSqlParseException(String.format("【UPDATE-UPDATE】回滚语句生成失败，没有where条件。原sql:%s", SQLUtils.toSQLString(statement,dbType)));
				}
			}
			if(deleteBuilder != null) {
				if(deleteBuilder.getSQLDeleteStatement().getWhere() == null) {
					throw new TxcSqlParseException(String.format("【INSERT-DELETE】回滚语句生成失败，没有where条件。原sql:%s", SQLUtils.toSQLString(statement,dbType)));
				}
			}
			result.pipeline = pipeline;
			((SQLSelectQueryBlock)queryBuilder.getSQLSelectStatement().getSelect().getQuery()).setForUpdate(true);//增加for update
			result.querySql = queryBuilder.getSQLSelectStatement();
			result.tableName = tableName;
			result.where = null;//FIXME
			logger.debug("原sql:{} 解析出来的sql:{}",SQLUtils.toSQLString(statement,dbType),SQLUtils.toSQLString(result.rollbackSql,dbType));
			return result;
		} finally {
			context.stop();
		}
	}

	class TxcSQLASTVisitorAdivce extends SQLASTVisitorAspectAdapter {
		
		int index = 0;//下标重排
		TableSchema schema;
		
		SQLBinaryOpExpr newWhere4Update;
		Set<String> alreadyDealPrimaryKeyStrs;
		
		/* 
		 * 解析table名
		 */
		@Override
		public SQLExprTableSource tableAspect(SQLExprTableSource table) {
			SQLIdentifierExpr tableName = (SQLIdentifierExpr) table.getExpr();
			String tablename = tableName.getName().toLowerCase();
			queryBuilder.from(tablename,table.getAlias());
			if(updateBuilder != null) {
				updateBuilder.from(tablename, table.getAlias());
			}
			if(deleteBuilder != null) {
				deleteBuilder.from(tablename,table.getAlias());
			}
			if(insertStatementBuilder != null) {
				insertStatementBuilder.insert(tablename);
			}
			TxcSqlParserProcessor.this.tableName = tablename;
			TableSchema schema = LRUCache.getTableSchema(tablename,new TableSchemaCacheLoader(jdbcTemplate, tablename,datasource));
			this.schema = schema;
			return table;
		}
		
		@Override
		public void deleteEnterPoint(SQLDeleteStatement deleteStatement) {
			
			List<String> columns = schema.getColumns();
			insertStatementBuilder.column(columns.toArray(new String[] {}));
			
			for(int i = 0;i < columns.size();i++) {
				final int idex = i;
				final String cname = columns.get(i).toLowerCase();
				pipeline.addExtractor(new SQLParamExtractorPipeline.UpdateSetItemExtractor(pipeline) {
					
					@Override
					public void extract() {
						Object value = this.getQueryColumn(cname);
						this.addFinalArg(idex,value);
					}
					
					@Override
					public String desc() {
						return String.format("【DELETE->INSERT】从原数据中抽取列【%s】的值", cname);
					}
				});
				index++;//让where条件下标后移
			}
			
			/** since 4.0.0 抽取主键字段及值*/
			result.primaryKeyExtractor = new PrimaryKeyExtractor(pipeline) {
				
				@Override
				public List<KeyValuePair> extract() {
					List<KeyValuePair> result = new ArrayList<>();
					for(String column : schema.getPK()) {
						KeyValuePair pair = new KeyValuePair(column,extractFromRecord(column));
						result.add(pair);
					}
					return result;
				}
				
				@Override
				public String desc() {
					StringBuilder sbuilder = new StringBuilder(String.format("【DELETE->INSERT】从原数据中获取列【%s】的值作为txc反查主键条件", StringUtils.join(schema.getPK(), ",")));
					if(schema.getShardField() == null) {
						sbuilder.append(";无法解析到分库字段配置");
					}else {
						sbuilder.append(String.format(";从原数据中获取列【%s】的值作为分库字段值", schema.getShardField()));
					}
					return sbuilder.toString();
				}

				@Override
				public String shard() {
					if(schema.getShardField() == null) return null;
					return extractFromRecord(schema.getShardField()).toString();
				}
			};
			
			SQLSelectQueryBlock query =  (SQLSelectQueryBlock) queryBuilder.getSQLSelectStatement().getSelect().getQuery();
			SQLSelectItem selectItem = new SQLSelectItem(new SQLAllColumnExpr());
			query.addSelectItem(selectItem);
		}
		
		/* 
		 * insert语句解析成delete,抽取主键值
		 */
		@Override
		public void insertEnterPoint(SQLInsertStatement insertStatement) {
			List<SQLExpr> columns = insertStatement.getColumns();
			List<SQLExpr> _values = insertStatement.getValues().getValues();
			
			Set<String> _columns = new HashSet<>();
			for(SQLExpr column : columns) {
				_columns.add(((SQLName) column).getSimpleName().toLowerCase());
			}
			List<String> matchedPK = schema.matchPK(_columns);
			logger.debug("{} 【INSERT】matchedPK主键条件:{}",SQLUtils.toSQLString(statement, dbType),matchedPK);
			
			if(matchedPK == null || matchedPK.size() <= 0 ) throw new TxcNotSupportException("insert statement中没有包含主键！");
			
			int i = 0;
			SQLExpr where = null;
			Map<String,SQLExpr> pkExpr = new HashMap<>();
			SQLExpr shardExpr = null;
			for(SQLExpr column : columns) {
				String _columnName = ((SQLName) column).getSimpleName().toLowerCase();
				
				if(schema.getShardField() != null && schema.getShardField().toLowerCase().equals(_columnName)) {
					shardExpr = _values.get(i);
					if(!(shardExpr instanceof SQLVariantRefExpr || shardExpr instanceof SQLLiteralExpr)) {
						throw new TxcSqlParseException("insert statement中包含不支持的参数类型，目前仅支持占位符和一般的SqlLiteralExpr参数类型");
					}
				}
				
				
				if(matchedPK.contains(_columnName)) {
					
					SQLVariantRefExpr right = new SQLVariantRefExpr("?");
					right.setIndex(index);
					final int tmpIndex = index;
					index++;
					
					if(_values.get(i) instanceof SQLVariantRefExpr) {
						SQLVariantRefExpr primaryKeyVariantRef = (SQLVariantRefExpr) _values.get(i);
						int _index = primaryKeyVariantRef.getIndex();
						pipeline.addExtractor(new SQLParamExtractorPipeline.OriginalParamExtractor(pipeline) {
							
							@Override
							public void extract() {
								Object[] args = (Object[]) context.getParam(SQLParamExtractorPipeline.OUTTER_ARGS);
								Object value = args[_index];
								this.addFinalArg(tmpIndex, value);
							}
							
							@Override
							public String desc() {
								return String.format("【INSERT->DELETE】从原始参数中提取主键值作为delete条件,下标为%d", _index);
							}
						});
						pkExpr.put(_columnName,(SQLVariantRefExpr) _values.get(i));
					} else if(_values.get(i) instanceof SQLLiteralExpr) {
						final String primaryKeyValue = _values.get(i).toString();
						pipeline.addExtractor(new SQLParamExtractorPipeline.OriginalParamExtractor(pipeline) {
							
							@Override
							public void extract() {
								this.addFinalArg(tmpIndex, primaryKeyValue);
							}
							
							@Override
							public String desc() {
								return String.format("【INSERT->DELETE】从原始SQL中提取主键值:%s作为条件", primaryKeyValue);
							}
						});
						pkExpr.put(_columnName,(SQLLiteralExpr)_values.get(i));
					} else {
						throw new TxcSqlParseException("insert statement中包含不支持的参数类型，目前仅支持占位符和一般的SqlLiteralExpr参数类型");
					}
					if(where != null) {
						SQLExpr andRight = new SQLBinaryOpExpr(new SQLIdentifierExpr(_columnName), SQLBinaryOperator.Equality, right);
						where = new SQLBinaryOpExpr(where, SQLBinaryOperator.BooleanAnd, andRight);
					}else {
						where = new SQLBinaryOpExpr(new SQLIdentifierExpr(_columnName), SQLBinaryOperator.Equality, right);
					}
				}
				i++;
			}
			
			if(where == null) {
				logger.warn("【INSERT-DELETE】原语句{}中没有解析到主键字段",SQLUtils.toSQLString(statement,dbType));
				throw new TxcTransactionException("txc insert->delete 没有解析到主键字段，不能执行");
			}
			
			if(shardExpr == null && schema.getShardField() != null) {
				logger.warn("当前语句解析不到分库字段值:表【{}】分库字段【{}】",tableName,schema.getShardField());
			}
			
			final SQLExpr shardExprTmp = shardExpr;
			
			if(shardExprTmp != null && schema.getShardField() != null) {//添加shard字段条件
				SQLVariantRefExpr right = new SQLVariantRefExpr("?");
				right.setIndex(index);
				final int tmpIndex = index;
				index++;
				if(shardExprTmp instanceof SQLVariantRefExpr) {
					SQLVariantRefExpr shardExprVariantRef = (SQLVariantRefExpr) shardExprTmp;
					final int _index = shardExprVariantRef.getIndex();
					pipeline.addExtractor(new SQLParamExtractorPipeline.OriginalParamExtractor(pipeline) {
						@Override
						public void extract() {
							Object[] args = (Object[]) context.getParam(SQLParamExtractorPipeline.OUTTER_ARGS);
							Object value = args[_index];
							this.addFinalArg(tmpIndex, value);
						}
						@Override
						public String desc() {
							return String.format("【INSERT->DELETE】从原始参数中提取分库字段作为delete条件,下标为%d", _index);
						}});
				} else if(shardExprTmp instanceof SQLLiteralExpr) {
					final String shardValue = deleteSingleQuote(shardExprTmp.toString());
					pipeline.addExtractor(new SQLParamExtractorPipeline.OriginalParamExtractor(pipeline) {
						@Override
						public void extract() {
							this.addFinalArg(tmpIndex, shardValue);
						}
						@Override
						public String desc() {
							return String.format("从sql中取值【%s】作为分库字段【%s】",shardValue,schema.getShardField());
						}});
				}
				where = new SQLBinaryOpExpr(where, SQLBinaryOperator.BooleanAnd, 
						new SQLBinaryOpExpr(new SQLIdentifierExpr(schema.getShardField()), SQLBinaryOperator.Equality, right));
			}
			
			deleteBuilder.getSQLDeleteStatement().setWhere(where);
			
			result.primaryKeyExtractor = new PrimaryKeyExtractor(pipeline) {
				@Override
				public List<KeyValuePair> extract() {
					List<KeyValuePair> result = new ArrayList<>();
					for(Entry<String, SQLExpr> expr : pkExpr.entrySet()) {
						if(expr.getValue() instanceof SQLVariantRefExpr) {
							int index = ((SQLVariantRefExpr)expr.getValue()).getIndex();
							result.add(new KeyValuePair(expr.getKey(), extractFromArgs(index)));
						}else if(expr.getValue() instanceof SQLLiteralExpr){
							String value = deleteSingleQuote(((SQLLiteralExpr)expr.getValue()).toString());
							result.add(new KeyValuePair(expr.getKey(), value));
						}
					}
					return result;
				}
				
				@Override
				public String desc() {
					StringBuilder sbuilder = new StringBuilder("【INSERT->DELETE】");
					for(Entry<String, SQLExpr> expr : pkExpr.entrySet()) {
						if(expr.getValue() instanceof SQLVariantRefExpr) {
							int index = ((SQLVariantRefExpr)expr.getValue()).getIndex();
							sbuilder.append(String.format("从原参数中取下标为【%d】作为txc反查主键条件【%s】;", index,expr.getKey()));
						}else if(expr.getValue() instanceof SQLLiteralExpr){
							sbuilder.append(String.format("从sql中取值【%s】作为txc反查主键条件【%s】;", deleteSingleQuote(((SQLLiteralExpr)expr.getValue()).toString()),expr.getKey()));
						}
					}
					if(schema.getShardField() == null) {
						sbuilder.append(String.format("无法解析到分库字段配置"));
					}else {
						if(shardExprTmp == null) {
							sbuilder.append(String.format("无法解析到分库字段【%s】",schema.getShardField()));
						} else {
							if(shardExprTmp instanceof SQLVariantRefExpr) {
								int index = ((SQLVariantRefExpr)shardExprTmp).getIndex();
								sbuilder.append(String.format("从原参数中取下标为【%d】作为分库字段【%s】",index,schema.getShardField()));
							} else if(shardExprTmp instanceof SQLLiteralExpr) {
								sbuilder.append(String.format("从sql中取值【%s】作为分库字段【%s】",deleteSingleQuote(((SQLLiteralExpr)shardExprTmp).toString()),schema.getShardField()));
							}
						}
					}
					return sbuilder.toString();
				}

				@Override
				public String shard() {
					if(shardExprTmp == null) {
						return null;
					} else {
						if(shardExprTmp instanceof SQLVariantRefExpr) {
							int index = ((SQLVariantRefExpr)shardExprTmp).getIndex();
							return extractFromArgs(index).toString();
						} else if(shardExprTmp instanceof SQLLiteralExpr) {
							return deleteSingleQuote(((SQLLiteralExpr)shardExprTmp).toString());
						} 
						return null;
					}
				}
			};
		}
		
		/* 
		 * update set部分
		 */
		@Override
		public SQLUpdateSetItem updateItemAspect(SQLUpdateSetItem updateItem) {
			
			SQLUpdateSetItem newItem = new SQLUpdateSetItem();
			newItem.setColumn(updateItem.getColumn());
			
			SQLName column = (SQLName) updateItem.getColumn();
			final String columnName = column.getSimpleName().toLowerCase();
			SQLExpr value = updateItem.getValue();
			
			
			/*主键更新	特殊处理--------------------------------------------------------------------------------------------start*/
			if(schema.getPK().contains(columnName)) {
				if(alreadyDealPrimaryKeyStrs == null) {
					alreadyDealPrimaryKeyStrs = new HashSet<>();
				}
				alreadyDealPrimaryKeyStrs.add(columnName);
				SQLVariantRefExpr right = new SQLVariantRefExpr("?");//更新字段中包含主键,需要在回滚语句中的where条件中取更新参数而不是取原数据
				right.setIndex(index);
				final int tmpIndex = index;
				index++;
				if(value instanceof SQLVariantRefExpr) {//case1: 	column1 = ?
					int oldIndex = ((SQLVariantRefExpr) value).getIndex();
					pipeline.addExtractor(new SQLParamExtractorPipeline.OriginalParamExtractor(pipeline) {
						@Override
						public void extract() {
							Object[] args = (Object[]) context.getParam(SQLParamExtractorPipeline.OUTTER_ARGS);
							Object value = args[oldIndex];
							this.addFinalArg(tmpIndex,value);
						}
						
						@Override
						public String desc() {
							return String.format("【WHERE】update语句存在主键更新，从原参数数组中获取下标【%d】的值作为回滚条件", oldIndex);
						}
					});
				}else if(value instanceof SQLLiteralExpr) {//case2:		column1 = 1;
					pipeline.addExtractor(new SQLParamExtractorPipeline.OriginalParamExtractor(pipeline) {
						@Override
						public void extract() {
							this.addFinalArg(tmpIndex,value.toString());
						}
						
						@Override
						public String desc() {
							return String.format("【WHERE】update语句存在主键更新，从原sql语句中获取值%s作为回滚条件,并放入下标【%d】", deleteSingleQuote(value.toString()),tmpIndex);
						}
					});
				}else if(value instanceof SQLMethodInvokeExpr) {//case3: 	column1 = now();
					throw new TxcNotSupportException("txc暂不支持的sql语法");
				}else if (value instanceof SQLIdentifierExpr || value instanceof SQLPropertyExpr) {
					SQLName valueColumn = (SQLName) value;
					final String valueColumnName = valueColumn.getSimpleName().toLowerCase();
					queryBuilder.select(valueColumnName);//额外查出valueColumn
					pipeline.addExtractor(new SQLParamExtractorPipeline.UpdateSetItemExtractor(pipeline) {
						
						@Override
						public void extract() {
							Object value = this.getQueryColumn(valueColumnName);
							this.addFinalArg(tmpIndex, value);
						}
						
						@Override
						public String desc() {
							return String.format("【WHERE】update语句存在主键更新,从原数据中获取列【%s】的值", valueColumnName);
						}
					});
				}else if(value instanceof SQLBinaryOpExpr) {//case4: 	column1 = column1 + 1; column1 = column1 + ?
					SQLName left = (SQLName) ((SQLBinaryOpExpr) value).getLeft();
					SQLBinaryOperator operator = ((SQLBinaryOpExpr) value).getOperator();
					SQLExpr _right = ((SQLBinaryOpExpr) value).getRight();
					if(!( _right instanceof SQLVariantRefExpr) && ! (_right instanceof SQLLiteralExpr)) {//column = now()
						throw new TxcNotSupportException("txc暂不支持的sql语法");
					}
					String leftField = left.getSimpleName();
					final String fieldToExtract;//待抽取的字段
					if(!leftField.toLowerCase().equals(columnName)) {//case4_1: column1 = column2 + xxx
						queryBuilder.select(leftField);//额外查出column2
						fieldToExtract = leftField;
					}else {
						fieldToExtract = columnName;
					}
					
					
					pipeline.addExtractor(new SQLParamExtractorPipeline.UpdateSetItemExtractor(pipeline) {
						
						private boolean type = (_right instanceof SQLVariantRefExpr) ? true : false;
						
						@Override
						public void extract() {
							Object operateData = null;
							if(_right instanceof SQLVariantRefExpr) {//column1 = column1 + ?
								int oldIndex = ((SQLVariantRefExpr) _right).getIndex();//从原参数中抽取操作数
								Object[] args = (Object[]) context.getParam(SQLParamExtractorPipeline.OUTTER_ARGS);
								operateData = args[oldIndex];
							}else if(_right instanceof SQLLiteralExpr){//column1 = column1 + 1
								operateData = ((SQLLiteralExpr) _right).toString();//从原语句中获取操作数
							}else {
								throw new TxcNotSupportException("不支持的sql语法");
							}
							Object value = this.getQueryColumn(fieldToExtract);
							Object result = AviatorEvaluator.execute(value+operator.getName()+operateData);
							this.addFinalArg(tmpIndex,result);
						}
						
						@Override
						public String desc() {
							return String.format("【WHERE】update语句存在主键更新,从原数据中获取列【%s】的值 作为左操作数，运算符为【%s】,%s", fieldToExtract,
									operator.getName(),type ? String.format("从原参数中获取下标为【%d】的参数做为右操作数", ((SQLVariantRefExpr) _right).getIndex()) : 
										String.format("从原sql语句中获取【%s】做为右操作数", ((SQLLiteralExpr) _right).toString()));
						}
					});
				}
				
				if(newWhere4Update == null) {
					newWhere4Update = new SQLBinaryOpExpr(new SQLIdentifierExpr(columnName), SQLBinaryOperator.Equality, right);
				}else {
					newWhere4Update = new SQLBinaryOpExpr(newWhere4Update, SQLBinaryOperator.BooleanAnd, 
							new SQLBinaryOpExpr(new SQLIdentifierExpr(columnName), SQLBinaryOperator.Equality, right));
				}
			}
			/*主键更新	特殊处理--------------------------------------------------------------------------------------------end*/
			
			SQLExpr newValue = null;
			if(value instanceof SQLVariantRefExpr || value instanceof SQLLiteralExpr || value instanceof SQLMethodInvokeExpr || value instanceof SQLIdentifierExpr || value instanceof SQLPropertyExpr) {//column1 = ?; column1 = 1; column1 = now(); column1 = column2;
				queryBuilder.select(columnName);
				SQLVariantRefExpr tmp = new SQLVariantRefExpr();
				tmp.setName("?");
				tmp.setParent(newItem);
				tmp.setIndex(index);
				final int tmpIndex = index;
				pipeline.addExtractor(new SQLParamExtractorPipeline.UpdateSetItemExtractor(pipeline) {
					@Override
					public void extract() {
						Object value = this.getQueryColumn(columnName);
						this.addFinalArg(tmpIndex,value);
					}
					
					@Override
					public String desc() {
						return String.format("【UPDATE】从原数据中获取列【%s】的值", columnName);
					}
				});
				index ++;
				newValue = tmp;
			}else if(value instanceof SQLBinaryOpExpr) {
				//column1 = column1 + 1;column1 = column2 + 1;column1 = column1 + column2;t.column1 = t.column1 + 1;
				SQLBinaryOpExpr op = (SQLBinaryOpExpr) value;
				if((op.getLeft() instanceof SQLName && ((SQLName) op.getLeft()).getSimpleName().equals(columnName))
						&& 
					(op.getRight() instanceof SQLNumericLiteralExpr || op.getRight() instanceof SQLVariantRefExpr)
						&&
					(op.getOperator() == SQLBinaryOperator.Add || op.getOperator() == SQLBinaryOperator.Subtract)
						) {
					SQLBinaryOpExpr newOp = new SQLBinaryOpExpr();
					newOp.setLeft(op.getLeft());//left 延用
					SQLBinaryOperator operator = op.getOperator();//op	如果是+、-操作则取反
					if(operator == SQLBinaryOperator.Add) {
						newOp.setOperator(SQLBinaryOperator.Subtract);
					}else if(operator == SQLBinaryOperator.Subtract) {
						newOp.setOperator(SQLBinaryOperator.Add);
					}
					if(op.getRight() instanceof SQLVariantRefExpr) {
						SQLVariantRefExpr _v = new SQLVariantRefExpr();
						_v.setName("?");
						_v.setParent(newOp);
						_v.setIndex(index);
						final int tmpIndex = index;//right	包含占位符
						pipeline.addExtractor(new SQLParamExtractorPipeline.OriginalParamExtractor(pipeline) {
							@Override
							public void extract() {
								Object[] args = (Object[]) context.getParam(SQLParamExtractorPipeline.OUTTER_ARGS);
								Object value = args[((SQLVariantRefExpr)op.getRight()).getIndex()];
								this.addFinalArg(tmpIndex,value);
							}
							
							@Override
							public String desc() {
								return String.format("【UPDATE】从原参数数组中获取下标【%d】的值", ((SQLVariantRefExpr)op.getRight()).getIndex());
							}
						});
						index ++;
						newOp.setRight(_v);
					}else {
						newOp.setRight(op.getRight());//right	直接取值
					}
					newValue = newOp;
				}else {
					queryBuilder.select(columnName);
					SQLVariantRefExpr tmp = new SQLVariantRefExpr();
					tmp.setName("?");
					tmp.setParent(newItem);
					tmp.setIndex(index);
					final int tmpIndex = index;
					pipeline.addExtractor(new SQLParamExtractorPipeline.UpdateSetItemExtractor(pipeline) {
						@Override
						public void extract() {
							Object value = this.getQueryColumn(columnName);
							this.addFinalArg(tmpIndex,value);
						}
						
						@Override
						public String desc() {
							return String.format("【UPDATE】从原数据中获取列【%s】的值", columnName);
						}
					});
					index ++;
					newValue = tmp;
				}
			}
			newItem.setValue(newValue);
			updateBuilder.getSQLUpdateStatement().addItem(newItem);
			return updateItem;
		}
		
		/* 
		 * 对where中的占位符重排下标
		 */
		@Override
		public void whereBinaryOpAspect(SQLVariantRefExpr right) {
			final int oldIndex = right.getIndex();
			right.setIndex(index);
			final int tmpIndex = index;
			pipeline.addExtractor(new SQLParamExtractorPipeline.OriginalParamExtractor(pipeline) {
				@Override
				public void extract() {
					Object[] args = (Object[]) context.getParam(SQLParamExtractorPipeline.OUTTER_ARGS);
					Object value = args[oldIndex];
					this.addFinalArg(tmpIndex,value);//delete生成insert回滚时不需要where条件
				}
				
				@Override
				public String desc() {
					return String.format("【QUERY-WHERE】从原参数数组中获取下标【%d】的值", oldIndex);
				}
			});
			index++;
		}

		/* 
		 * where解析比较复杂，延用原sql中的where结构
		 */
		@Override
		public void whereEnterPoint(SQLBinaryOpExpr x) {
			SQLSelectQueryBlock query =  (SQLSelectQueryBlock) queryBuilder.getSQLSelectStatement().getSelect().getQuery();
			query.setWhere(x);//查询语句延用老的where条件，不破坏原条件
			
			if(updateBuilder != null) {//回滚语句统一用主键代替原条件,并且只有update操作才会走到此处，insert语句没有where,delete语句有where但回滚语句中用不到
				
				queryBuilder.select(schema.getShardField());//查询分库字段值，如果schema.getShardField == null 则不查询
				
				for(String primaryKey : schema.getPK()) {
					
					queryBuilder.select(primaryKey);
					
					if(alreadyDealPrimaryKeyStrs!= null && alreadyDealPrimaryKeyStrs.contains(primaryKey)) {//已经在updateItemAspect中处理过的主键不再处理
						continue;
					}
					SQLVariantRefExpr right = new SQLVariantRefExpr("?");
					right.setIndex(index);
					final int tmpIndex = index;
					index++;
					pipeline.addExtractor(new SQLParamExtractorPipeline.UpdateSetItemExtractor(pipeline) {
						@Override
						public void extract() {
							Object value = this.getQueryColumn(primaryKey);
							this.addFinalArg(tmpIndex,value);
						}
						@Override
						public String desc() {
							return String.format("【UPDATE-WHERE】从原数据中获取列【%s】的值作为回滚语句中的主键条件", primaryKey);
						}
					});
					if(newWhere4Update == null) {
						newWhere4Update = new SQLBinaryOpExpr(new SQLIdentifierExpr(primaryKey),SQLBinaryOperator.Equality,right);
					}else {
						newWhere4Update = new SQLBinaryOpExpr(newWhere4Update,SQLBinaryOperator.BooleanAnd,
								new SQLBinaryOpExpr(new SQLIdentifierExpr(primaryKey),SQLBinaryOperator.Equality,right));
					}
				}
				
				if(schema.getShardField() != null) {//添加shard查询条件
					SQLVariantRefExpr right = new SQLVariantRefExpr("?");
					right.setIndex(index);
					final int tmpIndex = index;
					index++;
					newWhere4Update = new SQLBinaryOpExpr(newWhere4Update,SQLBinaryOperator.BooleanAnd,
							new SQLBinaryOpExpr(new SQLIdentifierExpr(schema.getShardField()),SQLBinaryOperator.Equality,right)); 
					
					pipeline.addExtractor(new SQLParamExtractorPipeline.UpdateSetItemExtractor(pipeline) {
						@Override
						public void extract() {
							Object value = this.getQueryColumn(schema.getShardField());
							this.addFinalArg(tmpIndex,value);
						}
						@Override
						public String desc() {
							return String.format("【UPDATE-WHERE】从原数据中获取列【%s】的值作为回滚语句中的分库条件", schema.getShardField());
						}
					});
				}
				
				result.primaryKeyExtractor = new PrimaryKeyExtractor(pipeline) {
					
					@Override
					public List<KeyValuePair> extract() {
						List<KeyValuePair> result = new ArrayList<>();
						for(String column : schema.getPK()) {
							KeyValuePair pair = new KeyValuePair(column,extractFromRecord(column));
							result.add(pair);
						}
						return result;
					}
					
					@Override
					public String desc() {
						StringBuilder sbuilder = new StringBuilder(String.format("【UPDATE】从原数据中获取列【%s】的值作为txc反查主键条件", StringUtils.join(schema.getPK(), ",")));
						if(schema.getShardField() == null) {
							sbuilder.append(";无法解析到分库字段配置");
						}else {
							sbuilder.append(String.format(";从原数据中获取列【%s】的值作为分库字段值", schema.getShardField()));
						}
						return sbuilder.toString();
					}

					@Override
					public String shard() {
						if(schema.getShardField() == null) return null;
						return extractFromRecord(schema.getShardField()).toString();
					}
				};
				updateBuilder.getSQLUpdateStatement().setWhere(newWhere4Update);
			}
		}
	}
	
	private static String deleteSingleQuote(String item) {
		if(item.startsWith("'") && item.endsWith("'")) {
			item = StringUtils.substringBeforeLast(StringUtils.substringAfter(item, "'"), "'");
		}
		return item;
	}

	@Override
	public boolean manual() {
		return false;
	}

	@Override
	public boolean auto() {
		return true;
	}
}

