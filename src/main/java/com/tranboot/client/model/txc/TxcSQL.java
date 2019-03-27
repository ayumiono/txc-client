package com.tranboot.client.model.txc;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import com.tranboot.client.model.SQLType;
import com.tranboot.client.model.txc.SQLParamExtractorPipeline.KeyValuePair;

public interface TxcSQL {
	
	public List<RollbackSqlInfo> rollbackSql(Object[] args,JdbcTemplate jdbc);
	
	public SQLType getSqlType();
	
	public String getTableName();
	
	public static class RollbackSqlInfo {
		public final String rollbackSql;
		public final List<KeyValuePair> primaryKVPair;
		public final String shardValue;
		
		public RollbackSqlInfo(String rollbackSql,List<KeyValuePair> primaryKVPair,String shardValue) {
			this.rollbackSql = rollbackSql;
			this.primaryKVPair = primaryKVPair;
			this.shardValue = shardValue;
		}
		
		public String pkv() {
			StringBuilder sbuilder = new StringBuilder();
			for(KeyValuePair kv : primaryKVPair) {
				sbuilder.append(kv.column).append("[").append(kv.value.toString()).append("]").append("-");
			}
			return StringUtils.substringBeforeLast(sbuilder.toString(),"-");
		}
	}
}
