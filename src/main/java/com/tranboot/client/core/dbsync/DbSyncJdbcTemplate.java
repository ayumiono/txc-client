package com.tranboot.client.core.dbsync;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.BatchUpdateUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import com.tranboot.client.core.dbsync.DbSyncSqlInvokeContextManager.SQLInvokeContext;
import com.tranboot.client.service.dbsync.DbSyncTaskBuilder;
import com.tranboot.client.utils.ThreadPoolUtils;

@Deprecated
public class DbSyncJdbcTemplate extends JdbcTemplate {
	
	@Autowired
	private DbSyncTaskBuilder taskBuilder;

	@Override
	public int update(String sql) throws DataAccessException {
		int effectRows = super.update(sql);
		SQLInvokeContext context = new SQLInvokeContext(sql, null);
		if(DbSyncSqlInvokeContextManager.inTransaction()) {
			DbSyncSqlInvokeContextManager.addSqlInvokeContext(context);
		}else {
			//不存在事务，直接提交SQL解析任务
			ThreadPoolUtils.submit(taskBuilder.build(DbSyncSqlInvokeContextManager.packWithoutTransaction(context)));
		}
		return effectRows;
	}

	@Override
	public int update(String sql, Object... args) throws DataAccessException {
		int effectRows = super.update(sql, args);
		SQLInvokeContext context = new SQLInvokeContext(sql, args);
		if(DbSyncSqlInvokeContextManager.inTransaction()) {
			DbSyncSqlInvokeContextManager.addSqlInvokeContext(context);
		}else {
			//不存在事务，直接提交SQL解析任务
			ThreadPoolUtils.submit(taskBuilder.build(DbSyncSqlInvokeContextManager.packWithoutTransaction(context)));
		}
		return effectRows;
	}

	@Override
	public int[] batchUpdate(String sql, final BatchPreparedStatementSetter pss) throws DataAccessException {
		return super.batchUpdate(sql, pss);
	}

	@Override
	public int[] batchUpdate(String sql, List<Object[]> batchArgs, int[] argTypes) throws DataAccessException {
		return BatchUpdateUtils.executeBatchUpdate(sql, batchArgs, argTypes, this);
	}
}
