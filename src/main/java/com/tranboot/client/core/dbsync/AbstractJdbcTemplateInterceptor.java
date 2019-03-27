package com.tranboot.client.core.dbsync;

import org.aopalliance.intercept.MethodInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tranboot.client.core.dbsync.DbSyncSqlInvokeContextManager.SQLInvokeContext;
import com.tranboot.client.service.dbsync.DbSyncTaskBuilder;
import com.tranboot.client.utils.ThreadPoolUtils;

public abstract class AbstractJdbcTemplateInterceptor implements MethodInterceptor {
	
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private DbSyncTaskBuilder taskBuilder;
	
	public AbstractJdbcTemplateInterceptor(DbSyncTaskBuilder taskBuilder) {
		this.taskBuilder = taskBuilder;
	}

	protected final void deal(String sql,Object[] args) {
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

}
