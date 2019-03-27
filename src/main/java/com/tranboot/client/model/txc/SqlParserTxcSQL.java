package com.tranboot.client.model.txc;

import static com.tranboot.client.utils.MetricsReporter.deal;
import static com.tranboot.client.utils.MetricsReporter.query;
import static com.tranboot.client.utils.MetricsReporter.render;
import static com.tranboot.client.utils.MetricsReporter.rollbackSqlTimer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.codahale.metrics.Timer;
import com.tranboot.client.druid.sql.SQLUtils;
import com.tranboot.client.druid.sql.SQLUtils.FormatOption;
import com.tranboot.client.druid.sql.ast.SQLStatement;
import com.tranboot.client.exception.TxcQueryException;
import com.tranboot.client.exception.TxcSqlParseException;
import com.tranboot.client.model.SQLType;
import com.tranboot.client.model.txc.SQLParamExtractorPipeline.PrimaryKeyExtractor;
import com.tranboot.client.sqlast.MySQLRewriteVistorAop;
import com.tranboot.client.sqlast.SQLASTVisitorAspectAdapter;
/**
 * 
 * 分布式事务用
 * 
 * 为了保证可缓存需要：
 *  1)update table field = field + ? ---> update table field = field - ? 
 *  2)update table field = field + 1 ---> update table field = field - ? 不能写死1
 *  3)update table field = 'field_value' ---> update table field = ? 不能写死'field_value' 
 *  	3-2)update table field = 1 ---> update table field=? 不能写死'1' 
 *  4)update table field = ? ---> update table field = ? 
 *  5)insert table(primaryKey,field1,field2) values('primaryId','field1_value','field2_value') ---> delete from table where primaryKey=?
 * 
 * 即所有不是sqlidentifyexpr和sqlpropertyexp和sqlvariantexp类型的都要用占位符替换
 * update中的where中必须包括主键，insert中必须插入主键
 * 
 * 占位符对应的参数有两种可能来源： 从querySql中查出的: 如上面的 3,4 从源sql语句中解析出来的:如上面的 2 从参数中带过来的:如上面的
 * 1,5
 * 
 * Thread Safe
 * @author xuelong.chen
 *
 */
public class SqlParserTxcSQL implements TxcSQL{
	
	/**
	 * 查询语句
	 */
	protected SQLStatement querySql;
	/**
	 * 回滚语句
	 */
	protected SQLStatement rollbackSql;
	/**
	 * where部分
	 */
	protected String where;
	/**
	 * update or insert or something else
	 */
	protected SQLType sqlType;
	
	protected String tableName;
	
	protected PrimaryKeyExtractor primaryKeyExtractor;
	
	/**
	 * 参数提取器
	 */
	protected SQLParamExtractorPipeline pipeline;

	private void deal(Object[] args) {
		Timer.Context context = deal.time();
		try {
			pipeline.start(args);// 提取where参数
		} finally {
			context.stop();
		}
	}

	private List<Map<String, Object>> query(JdbcTemplate jdbcTemplate) {
		Timer.Context context = query.time();
		String sql = querySql();
		try {
			List<Map<String, Object>> rows = jdbcTemplate.query(sql, new ColumnMapRowMapper());
			return rows;
		} catch (Exception e) {
			throw new TxcQueryException(e, String.format("%s 查询失败", sql));
		} finally {
			context.stop();
		}
	}

	private void render(Map<String, Object> row) {
		Timer.Context context = render.time();
		try {
			pipeline.setParam(SQLParamExtractorPipeline.QUERY_ROW, row);
			pipeline.render();// 提取row中的参数
		} finally {
			context.stop();
		}
	}

	private RollbackSqlInfo rollbackSql() {
		StringBuilder sbuilder = new StringBuilder();
		rollbackSql.accept(new MySQLRewriteVistorAop(pipeline.getFinalArgs(), sbuilder, new SQLASTVisitorAspectAdapter()));
		String sql = sbuilder.toString();
		return new RollbackSqlInfo(sql,primaryKeyExtractor.render(),primaryKeyExtractor.shard());
	}
	
	private String querySql() {
		try {
			StringBuilder sbuilder = new StringBuilder();
			querySql.accept(new MySQLRewriteVistorAop(pipeline.getFinalArgs(), sbuilder, new SQLASTVisitorAspectAdapter()));
			return sbuilder.toString();
		} catch (Exception e) {
			throw new TxcSqlParseException(e, String.format("%s 生成查询语句失败", querySql.toString()));
		} finally {
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public SQLType getSqlType() {
		return sqlType;
	}

	public void setSqlType(SQLType sqlType) {
		this.sqlType = sqlType;
	}
	
	@Override
	public List<RollbackSqlInfo> rollbackSql(Object[] args, JdbcTemplate jdbc) {
		Timer.Context context = rollbackSqlTimer.time();
		try {
			List<RollbackSqlInfo> rollbackSqls = new ArrayList<>();
			deal(args);
			if(querySql == null) {//如果不需要查询
				render(null);
				rollbackSqls.add(rollbackSql());
			}else {
				if(this.sqlType == SQLType.INSERT) {
					return Collections.singletonList(rollbackSql());
				}
				List<Map<String, Object>> rows = query(jdbc);
				for(Map<String, Object> row : rows) {
					render(row);
					rollbackSqls.add(rollbackSql());
				}
			}
			return rollbackSqls;
		} catch (Exception e) {
			throw new TxcSqlParseException(e, String.format("%s 生成回滚语句失败", rollbackSql.toString()));
		} finally {
			context.stop();
		}
	}
	
	@Override
	public String toString() {
		try {
			StringBuilder sbuilder = new StringBuilder();
			sbuilder.append("生成方式:").append("自动").append(System.lineSeparator());
			sbuilder.append("查询语句:").append(querySql == null ? "无需前置查询" : SQLUtils.toMySqlString(querySql,new FormatOption(false, false, false))).append(System.lineSeparator());
			sbuilder.append("回滚语句:").append(SQLUtils.toMySqlString(rollbackSql,new FormatOption(false, false, false))).append(System.lineSeparator());
			sbuilder.append("主键抽取器:").append(primaryKeyExtractor.toString()).append(System.lineSeparator());
			sbuilder.append("处理拓扑:").append(System.lineSeparator()).append(pipeline.toString()).append(System.lineSeparator());
			return sbuilder.toString();
		} finally {
		}
	}
}
