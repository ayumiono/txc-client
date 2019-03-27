package com.tranboot.client.core.dbsync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbSyncSqlInvokeContextManager {
	
	private static final Logger logger = LoggerFactory.getLogger(DbSyncSqlInvokeContextManager.class);
	
	protected static final ThreadLocal<TransactionPack> sqlInvokeContexts = new ThreadLocal<>();
	
	protected static final void beginTransaction(String transactionNo) {
		TransactionPack _cache = sqlInvokeContexts.get();
		if(_cache == null) {
			sqlInvokeContexts.set(new TransactionPack(transactionNo));
		}else {
			logger.error("【DbSync】事务上下文错误");
		}
	}
	
	protected static final boolean inTransaction() {
		TransactionPack _cache = sqlInvokeContexts.get();
		if(_cache == null) {
			return false;
		}else {
			return true;
		}
	}
	
	protected static final TransactionPack get() {
		return sqlInvokeContexts.get();
	}
	
	protected static final int size() {
		return sqlInvokeContexts.get() == null ? 0 : sqlInvokeContexts.get().size();
	}
	
	protected static final int addSqlInvokeContext(SQLInvokeContext context) {
		TransactionPack _cache = sqlInvokeContexts.get();
		if(_cache == null) {
			logger.error("【DbSync】没有开启事务");
		}
		sqlInvokeContexts.get().addSqlInokeContext(context);
		return sqlInvokeContexts.get().size();
	}
	
	public static TransactionPack packWithoutTransaction(SQLInvokeContext...contexts) {
		TransactionPack pack = new TransactionPack("NULL_TRAN");
		pack.setSqlinvokecontexts(Arrays.asList(contexts));
		return pack;
	}
	
	public static class TransactionPack {
		protected final String transactionNo;
		protected List<SQLInvokeContext> sqlinvokecontexts = new ArrayList<>();
		
		protected TransactionPack(String transactionNo) {
			this.transactionNo = transactionNo;
		}
		
		protected void addSqlInokeContext(SQLInvokeContext context) {
			sqlinvokecontexts.add(context);
		}
		
		protected int size() {
			return sqlinvokecontexts.size();
		}

		public List<SQLInvokeContext> getSqlinvokecontexts() {
			return sqlinvokecontexts;
		}

		public void setSqlinvokecontexts(List<SQLInvokeContext> sqlinvokecontexts) {
			this.sqlinvokecontexts = sqlinvokecontexts;
		}

		public String getTransactionNo() {
			return transactionNo;
		}
	}
	
	public static class SQLInvokeContext{
		
		protected String sql;
		protected Object[] args;
		public SQLInvokeContext(String sql,Object[] args) {
			this.sql = sql;
			this.args = args;
		}
		public String getSql() {
			return sql;
		}
		public void setSql(String sql) {
			this.sql = sql;
		}
		public Object[] getArgs() {
			return args;
		}
		public void setArgs(Object[] args) {
			this.args = args;
		}
	}
	
	protected static final void releaseResources() {
		sqlInvokeContexts.remove();
	}
}
