package com.tranboot.client.core.txc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.aopalliance.intercept.MethodInvocation;
import org.springframework.jdbc.core.JdbcTemplate;

import com.codahale.metrics.Timer;
import com.gb.soa.omp.transaction.model.SqlParamModel;
import com.tranboot.client.exception.TxcDBException;
import com.tranboot.client.exception.TxcTransactionException;
import com.tranboot.client.model.DBType;
import com.tranboot.client.model.SQLType;
import com.tranboot.client.model.txc.BranchRollbackInfo;
import com.tranboot.client.model.txc.BranchRollbackMessage;
import com.tranboot.client.model.txc.TxcSQL;
import com.tranboot.client.model.txc.TxcSQL.RollbackSqlInfo;
import com.tranboot.client.model.txc.TxcSQLTransformer.TxcSQLTransform;
import com.tranboot.client.model.txc.TxcSqlProcessorWraper;
import com.tranboot.client.service.txc.TxcRedisService;
import com.tranboot.client.utils.LRUCache;

/**
 * interceptor for
 * org.springframework.jdbc.core.JdbcTemplate.batchUpdate(java.lang.String,java.util.List<java.lang.Object[]>,int[])
 * org.springframework.jdbc.core.JdbcTemplate.batchUpdate(java.lang.String,java.util.List<java.lang.Object[]>)
 * 
 * @author xuelong.chen
 *
 */
public class TxcJdbcTemplateInterceptor4 extends AbstractJdbcTemplateInterceptor {

	public TxcJdbcTemplateInterceptor4(String datasource, DataSource ds, TxcRedisService txcRedisService,
			DBType dbType) {
		super(datasource, ds, txcRedisService, dbType);
	}

	public Object txc(MethodInvocation invocation, BranchRollbackMessage message, final int transactionType,final List<ReentrantRedLock> locks)
			throws TxcDBException {
		
		JdbcTemplate proxyedObject = (JdbcTemplate)invocation.getThis();
		try {
			Object[] arguments = invocation.getArguments();
			String sql = (String) arguments[0];
			
			String txc = TxcRollbackSqlManagerV2.txc(message);
			
			TxcSQLTransform transformed = LRUCache.getTxcTransformedSql(sql);
			
			TxcSQL txcSql = LRUCache.getTxcSql(transformed.sql,new Callable<TxcSQL>() {
				@Override
				public TxcSQL call() throws Exception {
					return new TxcSqlProcessorWraper(transformed.sql, proxyedObject,dbType,datasource).parse();
				}});
			
			if(transformed.sqlType != SQLType.DELETE) {
				final TxcSQLTransform finalTransform = transformed;
				final String finalTxc = txc;
				arguments[0] = transformed.sql;
				@SuppressWarnings("unchecked")
				List<Object[]> args = (List<Object[]>) arguments[1];
				arguments[1] = args.stream().map(new Function<Object[], Object[]>() {
					@Override
					public Object[] apply(Object[] t) {
						return finalTransform.addArgs(t, finalTxc);
					}
				}).collect(Collectors.toList());
			}
			
			@SuppressWarnings("unchecked")
			List<Object[]> args = (List<Object[]>) arguments[1];
			List<BranchRollbackInfo> rollbacks = new ArrayList<>();
			for (int i = 0; i < args.size(); i++) {
				BranchRollbackInfo rollbackInfo = new BranchRollbackInfo();
				List<SqlParamModel> redisModel = new ArrayList<>();
				
				List<RollbackSqlInfo> rollbackSqls = txcSql.rollbackSql(args.get(i), proxyedObject);
				for (RollbackSqlInfo rollbackSql : rollbackSqls) {
					String lockKey = new StringBuilder(datasource).append("-").append(txcSql.getTableName())
							.append("-").append(rollbackSql.pkv()).toString();
					ReentrantRedLock lock = lock(lockKey,TxcContext.getTxcTimeout(),locks);
					SqlParamModel model = redisSqlModel(rollbackSql, txcSql, transactionType, txc, lockKey,lock.getRedLockValue());
					redisModel.add(model);
				}
				rollbackInfo.setRollbackSql(redisModel);
				rollbacks.add(rollbackInfo);
			}
			
			message.addBranchRollbackInfo(datasource,rollbacks);
			/** since 4.0.0 存redis操作前置到业务处理之前，并且每执行一条sql,覆盖一次redis */
			txcRedisService.hput(TxcRedisService.buildTransactionModel(message));
		} catch (Exception e) {
			throw new TxcTransactionException(e,e.getMessage());
		}
		
		Timer.Context context = processTimer.time();
		try {
			return invocation.proceed();
		} catch (Throwable e) {
			throw new TxcDBException(e,e.getMessage());
		} finally {
			context.stop();
		}
	}

}

