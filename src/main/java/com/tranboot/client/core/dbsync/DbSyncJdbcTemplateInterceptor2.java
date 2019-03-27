package com.tranboot.client.core.dbsync;

import org.aopalliance.intercept.MethodInvocation;

import com.tranboot.client.service.dbsync.DbSyncTaskBuilder;

/**
 * interceptor for
 * org.springframework.jdbc.core.JdbcTemplate.update(java.lang.String,java.lang.Object...)
 * org.springframework.jdbc.core.JdbcTemplate.update(java.lang.String,java.lang.Object[],int[])
 * @author xuelong.chen
 *
 */
public class DbSyncJdbcTemplateInterceptor2 extends AbstractJdbcTemplateInterceptor {

	public DbSyncJdbcTemplateInterceptor2(DbSyncTaskBuilder taskBuilder) {
		super(taskBuilder);
	}

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		Object result = invocation.proceed();
		Object[] args = invocation.getArguments();
		String sql = (String) args[0];
		Object[] params = (Object[]) args[1];
		deal(sql, params);
		return result;
	}

}
