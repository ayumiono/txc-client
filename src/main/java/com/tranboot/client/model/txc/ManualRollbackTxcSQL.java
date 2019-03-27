package com.tranboot.client.model.txc;

import static com.tranboot.client.utils.MetricsReporter.manualRollbackSqlTimer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

import com.codahale.metrics.Timer;
import com.tranboot.client.druid.sql.SQLUtils;
import com.tranboot.client.druid.sql.ast.SQLStatement;
import com.tranboot.client.druid.util.JdbcUtils;
import com.tranboot.client.exception.TxcSqlParseException;
import com.tranboot.client.model.SQLType;
import com.tranboot.client.model.txc.SQLParamExtractorPipeline.KeyValuePair;
import com.tranboot.client.sqlast.MySQLRewriteVistorAop;
import com.tranboot.client.sqlast.SQLASTVisitorAspectAdapter;

public class ManualRollbackTxcSQL implements TxcSQL{
	
	public final String originalSql;
	public final String rollbackSql;
	public final int[] indexs;
	public final SQLStatement rollbackStatement;
	public final SQLType sqlType;
	public final String table;
	public final String[] primaryKey;
	public final int[] primaryValueIndex;
	public final Integer shardValueIndex;
	
	public ManualRollbackTxcSQL(String originalSql,String rollbackSql,int[] indexs,String table,SQLType sqlType,
			String[] primaryKey,int[] primaryValueIndex,int shardValueIndex) {
		this.originalSql = originalSql;
		this.rollbackSql = rollbackSql;
		this.indexs = indexs;
		this.rollbackStatement = SQLUtils.parseStatements(rollbackSql, JdbcUtils.MYSQL).get(0);
		this.sqlType = sqlType;
		this.table = table;
		this.primaryKey = primaryKey;
		this.primaryValueIndex = primaryValueIndex;
		this.shardValueIndex = shardValueIndex;
	}
	
	@Override
	public List<RollbackSqlInfo> rollbackSql(Object[] args,JdbcTemplate jdbc) {
		Timer.Context context = manualRollbackSqlTimer.time();
		Object[] params = null;
		if(indexs != null) {
			params = new Object[indexs.length];
			for(int i = 0;i < indexs.length;i++) {
				params[i] = args[indexs[i]];
			}
		}
		try {
			StringBuilder sbuilder = new StringBuilder();
			rollbackStatement.accept(new MySQLRewriteVistorAop(params == null ? new ArrayList<>() : Arrays.asList(params), sbuilder, new SQLASTVisitorAspectAdapter()));
			String rollbackSql = sbuilder.toString();
			List<KeyValuePair> primaryKVPair = new ArrayList<>();
			if(primaryKey != null && primaryKey.length > 0) {
				for(int i=0;i<primaryKey.length;i++) {
					primaryKVPair.add(new KeyValuePair(primaryKey[i], args[primaryValueIndex[i]]));
				}
			}
			RollbackSqlInfo rollbackInfo = new RollbackSqlInfo(rollbackSql,primaryKVPair,shardValueIndex == null ? null : args[shardValueIndex].toString());
			return Collections.singletonList(rollbackInfo);
		} catch (Exception e) {
			throw new TxcSqlParseException(e, String.format("%s 生成回滚语句失败", rollbackSql));
		} finally {
			context.stop();
		}
	}

	@Override
	public SQLType getSqlType() {
		return this.sqlType;
	}

	@Override
	public String getTableName() {
		return this.table;
	}
	
	@Override
	public String toString() {
		StringBuilder sbuilder = new StringBuilder();
		sbuilder.append("生成方式:").append("手动配置").append(System.lineSeparator());
		sbuilder.append("源语句:").append(originalSql).append(System.lineSeparator());
		sbuilder.append("回滚语句:").append(rollbackSql).append(System.lineSeparator());
		sbuilder.append("参数下标:").append(indexs == null ? "" : Arrays.toString(indexs)).append(System.lineSeparator());
		sbuilder.append("主键字段:").append(primaryKey).append(System.lineSeparator());
		sbuilder.append("主键参数下标:").append(primaryValueIndex).append(System.lineSeparator());
		return sbuilder.toString();
	}
}
