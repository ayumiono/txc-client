package com.tranboot.client.core.dbsync;

import java.util.List;

import org.aopalliance.intercept.MethodInvocation;

import com.tranboot.client.service.dbsync.DbSyncTaskBuilder;

/**
 * interceptor for 
 * org.springframework.jdbc.core.JdbcTemplate.batchUpdate(java.lang.String,java.util.List<java.lang.Object[]>,int[])
 * org.springframework.jdbc.core.JdbcTemplate.batchUpdate(java.lang.String,java.util.List<java.lang.Object[]>)
 * @author xuelong.chen
 *
 */
public class DbSyncJdbcTemplateInterceptor4 extends AbstractJdbcTemplateInterceptor {

	public DbSyncJdbcTemplateInterceptor4(DbSyncTaskBuilder taskBuilder) {
		super(taskBuilder);
	}

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		Object result = invocation.proceed();
		Object[] args = invocation.getArguments();
		String sql = (String) args[0];
		@SuppressWarnings("unchecked")
		List<Object[]> params = (List<Object[]>) args[1];
		for(Object[] arg : params) {
			deal(sql,arg);
		}
		return result;
	}

}
