package com.tranboot.client.core.dbsync;

import org.aopalliance.intercept.MethodInvocation;

import com.tranboot.client.service.dbsync.DbSyncTaskBuilder;

/**
 * interceptor for org.springframework.jdbc.core.JdbcTemplate.batchUpdate(java.lang.String...)
 * @author xuelong.chen
 *
 */
public class DbSyncJdbcTemplateInterceptor3 extends AbstractJdbcTemplateInterceptor {

	public DbSyncJdbcTemplateInterceptor3(DbSyncTaskBuilder taskBuilder) {
		super(taskBuilder);
	}

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		Object result = invocation.proceed();
		Object[] args = invocation.getArguments();
		String[] sqls = (String[]) args[0];
		for(String sql : sqls) {
			deal(sql,null);
		}
		return result;
	}

}
