package com.tranboot.client.core.txc;

import static com.tranboot.client.utils.MetricsReporter.throughput;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.gb.soa.omp.transaction.model.SqlParamModel;
import com.tranboot.client.exception.TxcDBException;
import com.tranboot.client.exception.TxcTransactionTimeoutException;
import com.tranboot.client.model.DBType;
import com.tranboot.client.model.txc.BranchRollbackMessage;
import com.tranboot.client.model.txc.TxcSQL;
import com.tranboot.client.model.txc.TxcSQL.RollbackSqlInfo;
import com.tranboot.client.service.txc.TxcRedisService;
import com.tranboot.client.spring.ContextUtils;
import com.tranboot.client.utils.BranchIdGenerator;
import com.tranboot.client.utils.MetricsReporter;

public abstract class AbstractJdbcTemplateInterceptor implements MethodInterceptor {
	
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	protected final Timer txcTimer = MetricsReporter.timer(MetricRegistry.name(this.getClass(),"txc"));
	protected final Timer processTimer = MetricsReporter.timer(MetricRegistry.name(this.getClass(), "process"));
	protected final Timer invokeTimer = MetricsReporter.timer(MetricRegistry.name(this.getClass(), "invoke"));
	
	protected final String datasource;
	protected final DataSource ds;
	protected final TxcRedisService txcRedisService;
	
	/**
	 * since 4.0.0 增加oracle支持
	 */
	protected final DBType dbType;
	
	public AbstractJdbcTemplateInterceptor(String datasource,DataSource ds,TxcRedisService txcRedisService,DBType dbType) {
		this.datasource = datasource;
		this.ds = ds;
		this.txcRedisService = txcRedisService;
		this.dbType = dbType;
	}
	
	/** since 4.0.0 每执行一条sql需要执行以下操作：
	 * 	1.放入到BranchRollbackMesssage实体中
	 * 	2.将BranchRollbackMessage更新到redis
	 * 	3.事务完成后执行BranchRollbackMessage 状态更新成1 ，并更新到redis
	 * 	所以如果一个事务有N条sql,则需要执行N+1次redis hput操作
	 * */
	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		Timer.Context invokeContext = invokeTimer.time();
		try {
			if(TxcContext.inTxcTransaction()) {
				if(!TxcRollbackSqlManagerV2.inTransaction(ds)) {
					BranchRollbackMessage message = new BranchRollbackMessage();
					message.setXid(TxcContext.getCurrentXid());
					long branchId = BranchIdGenerator.branchId();
					message.setDataSource(datasource);
					message.setBranchId(branchId);
					message.setStatus(TxcRedisService.INIT_STATUS);
					message.setServerIp(ContextUtils.getServerIp());
					message.setTransactionStartDate(new Date(TxcContext.getTxcStart()));
					message.setTransactionOutTimeSecond(TxcContext.getTxcTimeout() * 1000L);
					Timer.Context context = txcTimer.time();
					List<ReentrantRedLock> locks = new ArrayList<>();
					try {
						logger.debug(datasource+"不存在本地事务");
						long redisTime = txcRedisService.getCurrentTimeMillisFromRedis();
						if(TxcContext.getTxcStart() + TxcContext.getTxcTimeout() * 1000 < redisTime) {//FIXME 时间不准
							throw new TxcTransactionTimeoutException(String.format("分布式事务%d超时: 系统时间:%d 事务开始时间:%d 超时设置:%d", 
									TxcContext.getCurrentXid(),redisTime,TxcContext.getTxcStart(),TxcContext.getTxcTimeout() * 1000));
						}
						Object ret = txc(invocation,message,0,locks);
						throughput.mark();
						message.setStatus(TxcRedisService.COMMITTABLE_STATUS);
						txcRedisService.hput(TxcRedisService.buildTransactionModel(message));
						return ret;
					} catch (TxcDBException e) {
						message.setStatus(TxcRedisService.ROLLBACKABLE_STATUS);
						txcRedisService.hput(TxcRedisService.buildTransactionModel(message));
						throw e;
					} finally {
						for(ReentrantRedLock lock : locks) {
							lock.unlock();
						}
						context.stop();
					}
				}else {
					Timer.Context context = txcTimer.time();
					try {
						logger.debug(datasource+"存在本地事务");
						Object ret = txc(invocation,TxcRollbackSqlManagerV2.get(ds),1,null);
						throughput.mark();
						return ret;
					} catch (Throwable e) {
						throw e;
					} finally {
						context.stop();
					}
				}
			}else {
				Object ret = invocation.proceed();
				throughput.mark();
				return ret;
			}
		} finally {
			invokeContext.stop();
		}
		
	}
	
	/**
	 * 确保invocation.process()方法在txc()方法最后执行，保证redis中的status状态是正确的
	 * 因为，如果invocation.process()方法之后的代码报错，会导致数据库成功，但redis中的状态是rollback
	 * @param invocation
	 * @param message
	 * @param transactionType
	 * @param locks
	 * @return
	 * @throws Throwable
	 */
	public abstract Object txc(MethodInvocation invocation,BranchRollbackMessage message,final int transactionType,final List<ReentrantRedLock> locks) throws TxcDBException;

	protected static Object[] addArgs(Object[] args,Object... additionArgs) {
		return ArrayUtils.addAll(args, additionArgs);
	}
	
	/**
	 * 锁表记录
	 * @param key	行锁的KEY
	 * @param locks	存放集锁
	 * @return
	 */
	protected ReentrantRedLock lock(String key,int txcTimeout,final List<ReentrantRedLock> locks) {
		if(TxcRollbackSqlManagerV2.inTransaction(ds)) {
			return TxcRollbackSqlManagerV2.redlockInTransaction(ds, key, txcTimeout);
		}else {
			ReentrantRedLock lock = TxcRollbackSqlManagerV2.redlock(key,txcTimeout);
			locks.add(lock);
			return lock;
		}
	}
	
	protected SqlParamModel redisSqlModel(RollbackSqlInfo rollbacksql, TxcSQL txcSQL, int transactionType, String txc, String lockKey, String lockValue) {
		SqlParamModel model = new SqlParamModel();
		model.setTxc(txc);
		model.setSql(rollbacksql.rollbackSql);
		model.setTable(txcSQL.getTableName());
		model.setTransactionType(transactionType);
		model.setSqlType(txcSQL.getSqlType().getCode());
		model.setInsertRedisTime(System.currentTimeMillis());
		model.setTableKeyValueModels(JSON.toJSONString(rollbacksql.primaryKVPair));
		model.setRedisLockKey(lockKey);
		model.setRedisLockValue(lockValue);
		model.setShard(rollbacksql.shardValue);
		return model;
	}
}

