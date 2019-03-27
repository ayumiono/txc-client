package com.tranboot.client.core.dbsync;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.AopInvocationException;
import org.springframework.aop.RawTargetAccess;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.tranboot.client.core.dbsync.DbSyncSqlInvokeContextManager.TransactionPack;
import com.tranboot.client.service.dbsync.DbSyncTaskBuilder;
import com.tranboot.client.utils.BranchIdGenerator;
import com.tranboot.client.utils.ThreadPoolUtils;

/**
 * 
 * 数据库双写dsTransactionManager拦截器
 * @author xuelong.chen
 *
 */
public class DbSyncDataSourceTransactionManagerInterceptor implements MethodInterceptor {
	
	private static final Logger logger = LoggerFactory.getLogger(DbSyncDataSourceTransactionManagerInterceptor.class);

	private DbSyncTaskBuilder taskBuilder;
	
	private DataSourceTransactionManager target;

	public static final String getTransaction() {
		return "dbsync_transaction_" + BranchIdGenerator.branchId();
	}

	public DbSyncDataSourceTransactionManagerInterceptor(DataSourceTransactionManager target,DbSyncTaskBuilder taskBuilder) {
		this.target = target;
		this.taskBuilder = taskBuilder;
	}

	public Object invoke(Object proxy,Method method,Object[] args,MethodProxy methodProxy) throws Throwable {
		String name = method.getName();
		if ("doBegin".equals(name)) {
			Object retVal = methodProxy.invoke(this.target, args);
			retVal = processReturnType(proxy, this.target, method, retVal);
			String transactionId = getTransaction();
			logger.debug(String.format("开始数据库双写事务:%s",transactionId));
			DbSyncSqlInvokeContextManager.beginTransaction(transactionId);
			return retVal;
		} else if ("doCommit".equals(name)) {
			TransactionPack contexts = DbSyncSqlInvokeContextManager.sqlInvokeContexts.get();
			if(contexts != null) {
				logger.debug(String.format("开始提交:%s,存在%d条sql缓存记录",contexts.getTransactionNo(), DbSyncSqlInvokeContextManager.size()));
				ThreadPoolUtils.submit(taskBuilder.build(contexts));
			}
			DbSyncSqlInvokeContextManager.releaseResources();
			Object retVal = methodProxy.invoke(this.target, args);
			return processReturnType(proxy, this.target, method, retVal);
		} else if ("doRollback".equals(name)) {
			logger.debug("事务回滚," + (DbSyncSqlInvokeContextManager.get() == null ? "没有sql缓存记录" : String.format("存在%d条sql缓存记录", DbSyncSqlInvokeContextManager.size())));
			DbSyncSqlInvokeContextManager.releaseResources();
			Object retVal = methodProxy.invoke(this.target, args);
			return processReturnType(proxy, this.target, method, retVal);
		}
		return null;
	}

	@Override
	public Object intercept(Object proxy, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
		if ("doCommit".equals(method.getName()) || "doRollback".equals(method.getName())
				|| "doBegin".equals(method.getName())) {
			return invoke(proxy, method, args, methodProxy);
		}
		Object retVal = methodProxy.invoke(this.target, args);
		return processReturnType(proxy, this.target, method, retVal);
	}
	
	private static Object processReturnType(Object proxy, Object target, Method method, Object retVal) {
		if (retVal != null && retVal == target && !RawTargetAccess.class.isAssignableFrom(method.getDeclaringClass())) {
			retVal = proxy;
		}
		Class<?> returnType = method.getReturnType();
		if (retVal == null && returnType != Void.TYPE && returnType.isPrimitive()) {
			throw new AopInvocationException(
					"Null return value from advice does not match primitive return type for: " + method);
		}
		return retVal;
	}

}
