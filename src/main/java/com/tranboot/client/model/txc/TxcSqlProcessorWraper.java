package com.tranboot.client.model.txc;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import com.tranboot.client.druid.sql.SQLUtils;
import com.tranboot.client.druid.util.JdbcUtils;
import com.tranboot.client.model.DBType;
import com.tranboot.client.service.txc.TxcManualRollbackSqlService;
import com.tranboot.client.spring.ContextUtils;

public class TxcSqlProcessorWraper implements TxcSqlProcessor {
	
	private String sql;
	
	private TxcSqlProcessor processor;
	
	private DBType dbType;
	
	private JdbcTemplate jdbc;
	
	private String datasource;
	
	public TxcSqlProcessorWraper(String sql,JdbcTemplate jdbc,DBType dbType,String datasource) {
		this.sql = sql;
		this.jdbc = jdbc;
		this.dbType = dbType;
		this.datasource = datasource;
	}

	@Override
	public TxcSQL parse() {
		if(manual()) {
			logger.debug("{} 通过手动配置生成回滚语句。",sql);
			this.processor = new TxcSqlManualProcessor(sql);
		}else {
			this.processor = new TxcSqlParserProcessor(jdbc, SQLUtils.parseStatements(cleanSql(sql), dbType == DBType.ORACLE ? JdbcUtils.ORACLE : JdbcUtils.MYSQL).get(0),datasource);
		}
		return this.processor.parse();
	}

	@Override
	public boolean manual() {
		return ContextUtils.getBean(TxcManualRollbackSqlService.class).map(sql) != null;
	}

	@Override
	public boolean auto() {
		return ContextUtils.getBean(TxcManualRollbackSqlService.class).map(sql) == null;
	}

	protected String cleanSql(String sql) {
		//清洗掉mycat中的注解   /*!mycat:db_type=master*/update .....
		if(sql.indexOf("mycat")>-1) {
			return StringUtils.substringAfterLast(sql, "*/");
		}
		return sql;
	}
}
