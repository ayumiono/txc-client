package com.tranboot.client.core.dbsync;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.tranboot.client.core.dbsync.DbSyncSqlInvokeContextManager.SQLInvokeContext;
import com.tranboot.client.service.dbsync.DbSyncTaskBuilder;
import com.tranboot.client.utils.ThreadPoolUtils;

/**
 * 
 * 数据双写 jdbcTemplate拦截器
 * @author xuelong.chen
 * @deprecated 效率太差
 */
public class DbSyncJdbcTemplateInterceptor implements MethodInterceptor{
	
	private static final Logger logger = LoggerFactory.getLogger(DbSyncJdbcTemplateInterceptor.class);
	
	private DbSyncTaskBuilder taskBuilder;
	
	private static final List<String> methodFilter;
	static {
		methodFilter = new ArrayList<>();
		methodFilter.add("public int org.springframework.jdbc.core.JdbcTemplate.update(java.lang.String,java.lang.Object...) throws org.springframework.dao.DataAccessException");
		methodFilter.add("public int org.springframework.jdbc.core.JdbcTemplate.update(java.lang.String,java.lang.Object[],int[]) throws org.springframework.dao.DataAccessException");
		methodFilter.add("public int org.springframework.jdbc.core.JdbcTemplate.update(java.lang.String) throws org.springframework.dao.DataAccessException");
		methodFilter.add("public int[] org.springframework.jdbc.core.JdbcTemplate.batchUpdate(java.lang.String,java.util.List<java.lang.Object[]>,int[]) throws org.springframework.dao.DataAccessException");
		methodFilter.add("public int[] org.springframework.jdbc.core.JdbcTemplate.batchUpdate(java.lang.String,java.util.List<java.lang.Object[]>) throws org.springframework.dao.DataAccessException");
		methodFilter.add("public int[] org.springframework.jdbc.core.JdbcTemplate.batchUpdate(java.lang.String...) throws org.springframework.dao.DataAccessException");
	}
	
	public DbSyncJdbcTemplateInterceptor(DbSyncTaskBuilder taskBuilder) {
		this.taskBuilder = taskBuilder;
	}

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		String methodName = invocation.getMethod().toGenericString();
		Object result = invocation.proceed();
		Object[] args = invocation.getArguments();
		if(methodFilter.indexOf(methodName) == 0 || methodFilter.indexOf(methodName) == 1 || methodFilter.indexOf(methodName) == 2) {
			String sql = (String) args[0];
			Object[] params = args.length > 1 ? (Object[])args[1] : null;
			deal(sql,params);
		}else if(methodFilter.indexOf(methodName) == 5) {
			String[] sqls = (String[]) args[0];
			for(String sql : sqls) {
				deal(sql,null);
			}
		}else if(methodFilter.indexOf(methodName) == 3 || methodFilter.indexOf(methodName) == 4) {
			String sql = (String) args[0];
			@SuppressWarnings("unchecked")
			List<Object[]> params = (List<Object[]>) args[1];
			for(Object[] arg : params) {
				deal(sql,arg);
			}
		}else {
			return result;
		}
		return result;
	}

	private void deal(String sql,Object[] args) {
		SQLInvokeContext context = new SQLInvokeContext(sql, args);
		if(DbSyncSqlInvokeContextManager.inTransaction()) {
			int count = DbSyncSqlInvokeContextManager.addSqlInvokeContext(context);
			logger.debug(String.format("开启了事务，提交dbsync任务【%s】到线程本地缓存，当前持有sql invoke context数量为:%d", sql,count));
		}else {
			//不存在事务，直接提交SQL解析任务
			logger.debug("没有开启事务，直接提交dbsync任务");
			ThreadPoolUtils.submit(taskBuilder.build(DbSyncSqlInvokeContextManager.packWithoutTransaction(context)));
		}
	}
	
	public static void t(String...strings ) {
		System.out.println((String[]) strings);
	}
	
	public static void main(String[] args) {
		Method[]  methods = JdbcTemplate.class.getMethods();
		for(Method m : methods) {
			System.out.println(m.toGenericString());
		}
	}
}
