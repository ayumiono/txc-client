package com.tranboot.client.core.dbsync;

import org.aopalliance.intercept.MethodInvocation;

import com.tranboot.client.service.dbsync.DbSyncTaskBuilder;

/**
 * interceptor for org.springframework.jdbc.core.JdbcTemplate.update(java.lang.String)
 * @author xuelong.chen
 *
 */
public class DbSyncJdbcTemplateInterceptor1 extends AbstractJdbcTemplateInterceptor {

	public DbSyncJdbcTemplateInterceptor1(DbSyncTaskBuilder taskBuilder) {
		super(taskBuilder);
	}

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		Object result = invocation.proceed();
		Object[] args = invocation.getArguments();
		String sql = (String) args[0];
		deal(sql,null);
		return result;
	}

}
