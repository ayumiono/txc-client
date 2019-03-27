package com.tranboot.client.core.txc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

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
 * interceptor for org.springframework.jdbc.core.JdbcTemplate.update(java.lang.String)
 * @author xuelong.chen
 *
 */
public class TxcJdbcTemplateInterceptor1 extends AbstractJdbcTemplateInterceptor {

	public TxcJdbcTemplateInterceptor1(String datasource,DataSource ds,TxcRedisService txcRedisService,DBType dbType) {
		super(datasource, ds, txcRedisService,dbType);
	}
	
	public Object txc(MethodInvocation invocation,BranchRollbackMessage message,final int transactionType,final List<ReentrantRedLock> locks) throws TxcDBException {
		
		JdbcTemplate proxyedObject = (JdbcTemplate)invocation.getThis();
		try {
			BranchRollbackInfo rollbackInfo = new BranchRollbackInfo();
			List<SqlParamModel> redisModel = new ArrayList<>();
			
			String txc = TxcRollbackSqlManagerV2.txc(message);
			
			Object[] arguments = invocation.getArguments();
			String sql = (String) arguments[0];
			                                             
			TxcSQLTransform transformed = LRUCache.getTxcTransformedSql(sql);
			
			TxcSQL txcSql = LRUCache.getTxcSql(transformed.sql,new Callable<TxcSQL>() {
				@Override
				public TxcSQL call() throws Exception {
					return new TxcSqlProcessorWraper(transformed.sql, proxyedObject,dbType,datasource).parse();
				}
			});
			
			List<RollbackSqlInfo> rollbackSqls = new ArrayList<>();
			if(transformed.sqlType == SQLType.DELETE) {
				rollbackSqls = txcSql.rollbackSql(null, proxyedObject);
			}else {
				rollbackSqls = txcSql.rollbackSql(new Object[] {txc}, proxyedObject);
			}
			
			for(RollbackSqlInfo rollbackSql : rollbackSqls) {
				String lockKey = new StringBuilder(datasource).append("-").append(txcSql.getTableName())
						.append("-").append(rollbackSql.pkv()).toString();
				ReentrantRedLock lock = lock(lockKey,TxcContext.getTxcTimeout(),locks);
				SqlParamModel model = redisSqlModel(rollbackSql, txcSql, transactionType, txc, lockKey,lock.getRedLockValue());
				redisModel.add(model);
			}
			rollbackInfo.setRollbackSql(redisModel);
			message.addBranchRollbackInfo(datasource,rollbackInfo);
			/** since 4.0.0 存redis操作前置到业务处理之前，并且每执行一条sql,覆盖一次redis*/
			txcRedisService.hput(TxcRedisService.buildTransactionModel(message));
			
			Timer.Context context = processTimer.time();
			try {
				if(transformed.sqlType == SQLType.DELETE) {
					Object result = invocation.proceed();
					return result;
				}else {
					Object result = proxyedObject.update(transformed.sql, new Object[] {txc});//将update(sql) 方法转换成update(sql,args)
					return result;
				}
			} catch (Throwable e) {
				throw new TxcDBException(e, e.getMessage());
			} finally {
				context.stop();
			}
			
		} catch (Exception e) {
			throw new TxcTransactionException(e,e.getMessage());
		}
	}
}

