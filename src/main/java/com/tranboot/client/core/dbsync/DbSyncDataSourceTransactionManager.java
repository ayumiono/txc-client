package com.tranboot.client.core.dbsync;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionStatus;

import com.tranboot.client.core.dbsync.DbSyncSqlInvokeContextManager.TransactionPack;
import com.tranboot.client.service.dbsync.DbSyncTaskBuilder;
import com.tranboot.client.utils.ThreadPoolUtils;

@Deprecated
public class DbSyncDataSourceTransactionManager extends DataSourceTransactionManager {
	
	private static final long serialVersionUID = 311557423851944468L;
	
	public static final AtomicLong transatcionCounter = new AtomicLong();//branchId
	
	public static final String getTransaction() {
		return "dbsync_transaction_"+transatcionCounter.incrementAndGet();
	}
	
	@Autowired
	private DbSyncTaskBuilder taskBuilder;
	
	@Override
	protected void doBegin(Object transaction, TransactionDefinition definition) {
		super.doBegin(transaction, definition);
		DbSyncSqlInvokeContextManager.beginTransaction(getTransaction());
	}
	
	@Override
	protected void doCommit(DefaultTransactionStatus status) {
		TransactionPack contexts = DbSyncSqlInvokeContextManager.sqlInvokeContexts.get();
		if(contexts != null) {
			ThreadPoolUtils.submit(taskBuilder.build(contexts));
		}
		DbSyncSqlInvokeContextManager.releaseResources();
		super.doCommit(status);
	}

	protected void doRollback(DefaultTransactionStatus stats) {
		DbSyncSqlInvokeContextManager.releaseResources();
		if (stats.isCompleted()) {
			return;
		}
		super.doRollback(stats);
	}
}
