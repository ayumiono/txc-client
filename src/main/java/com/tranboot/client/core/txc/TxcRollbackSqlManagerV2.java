package com.tranboot.client.core.txc;

import static com.tranboot.client.utils.MetricsReporter.redLockFailCount;
import static com.tranboot.client.utils.MetricsReporter.redLockTimer;
import static com.tranboot.client.utils.MetricsReporter.redUnlockTimer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.codahale.metrics.Timer;
import com.tranboot.client.exception.TxcTransactionStatusError;
import com.tranboot.client.model.txc.BranchRollbackInfo;
import com.tranboot.client.model.txc.BranchRollbackMessage;
import com.tranboot.client.service.txc.TxcRedisService;
import com.tranboot.client.spring.ContextUtils;

/**
 * @author xuelong.chen
 *
 */
public final class TxcRollbackSqlManagerV2 {
	
	private static final Logger logger = LoggerFactory.getLogger(TxcRollbackSqlManagerV2.class);
	
	private static final Logger log = LoggerFactory.getLogger("txcRollbackSqlManager");
	
	protected static final ThreadLocal<Map<Object, BranchRollbackMessage>> _txcResource = new ThreadLocal<>();
	protected static final ThreadLocal<Map<Object, List<ReentrantRedLock>>> _redLocks = new ThreadLocal<>();
	
	/**
	 * 分布式事务行锁超时时间(毫秒)
	 */
	protected static final int TXC_LOCK_WIAT_TIMEOUT = 60 * 1000;
	
	protected final static String txc(BranchRollbackMessage message) {
		String txc = String.format("%s-%s", String.valueOf(message.getXid()), String.valueOf(message.getBranchId()));
		return txc;
	}
	
	protected static BranchRollbackMessage beginTransaction(DataSource ds,long branchId,long xid,String serverIp) {
		if(_txcResource.get() == null) {
			_txcResource.set(new HashMap<>());
		}
		if(_redLocks.get() == null) {
			_redLocks.set(new HashMap<>());
		}
		if(_txcResource.get().containsKey(ds) || _redLocks.get().containsKey(ds)) {
			logger.error("分布式事务状态错误 Expected Status is NULL,But Not NULL");
			releaseResource(ds);
		}
		BranchRollbackMessage message = new BranchRollbackMessage();
		message.setBranchId(branchId);
		message.setXid(xid);
		message.setServerIp(serverIp);
		message.setStatus(TxcRedisService.INIT_STATUS);
		message.setTransactionStartDate(new Date(TxcContext.getTxcStart()));
		message.setTransactionOutTimeSecond(TxcContext.getTxcTimeout() * 1000L);
		_txcResource.get().put(ds, message);
		_redLocks.get().put(ds, new ArrayList<>());
		return message;
	}
	
	protected static boolean inTransaction(DataSource ds) {
		if(_txcResource.get() == null) return false;
		if(_txcResource.get().containsKey(ds)) {
			return true;
		}
		return false;
	}
	
	protected static BranchRollbackMessage get(DataSource ds) {
		if(_txcResource.get() == null || !_txcResource.get().containsKey(ds)) {
			logger.error("分布式事务状态错误");
			throw new TxcTransactionStatusError("分布式事务状态错误 Expected txcResource is NOT NULL,But NULL");
		}
		return (BranchRollbackMessage) _txcResource.get().get(ds);
	}
	
	protected static ReentrantRedLock redlockInTransaction(DataSource ds,String key,int txcTimeout) {
		ReentrantRedLock reentrantRedLock = ReentrantRedLock.redisLock(ContextUtils.getBean(StringRedisTemplate.class), key, txcTimeout);
		Timer.Context context = redLockTimer.time();
		try {
			if(reentrantRedLock.tryLock(txcTimeout * 1000,TimeUnit.MILLISECONDS)) {
				if(_redLocks.get() == null || !_redLocks.get().containsKey(ds)) {
					context.stop();
					reentrantRedLock.unlock();
					logger.error("分布式事务状态错误");
					throw new TxcTransactionStatusError("分布式事务状态错误 Expected RedLock is NOT NULL,But NULL");
				}
				if(!_redLocks.get().get(ds).contains(reentrantRedLock)) {
					_redLocks.get().get(ds).add(reentrantRedLock);
				}
				return reentrantRedLock;
			}
		} finally {
			context.stop();
		}
		redLockFailCount.inc();
		throw new TxcTransactionStatusError(String.format("分布式事务锁【%s】获取超时!", key));
	}
	
	protected static ReentrantRedLock redlock(String key,int txcTimeout) {
		ReentrantRedLock reentrantRedLock = ReentrantRedLock.redisLock(ContextUtils.getBean(StringRedisTemplate.class), key, txcTimeout);
		Timer.Context context = redLockTimer.time();
		try {
			if(reentrantRedLock.tryLock(txcTimeout * 1000,TimeUnit.MILLISECONDS)) {
				return reentrantRedLock;
			}
		} finally {
			context.stop();
		}
		redLockFailCount.inc();
		throw new TxcTransactionStatusError(String.format("分布式事务锁【%s】获取超时!", key));
	}
	
	protected static void releaseResource(DataSource ds) {
		log.debug("开始释放分布式事务上下文...");
		if(_txcResource.get() != null) {
			_txcResource.get().remove(ds);
		}
		if(_redLocks.get() != null) {
			List<ReentrantRedLock> locks = _redLocks.get().get(ds);
			locks.forEach(new Consumer<ReentrantRedLock>() {
				@Override
				public void accept(ReentrantRedLock t) {
					Timer.Context context = redUnlockTimer.time();
					try {
						t.unlock();
					} finally {
						context.stop();
					}
				}
			});
			_redLocks.get().remove(ds);
		}
		log.debug("释放分布式事务上下文完成");
	}

	protected static void addBranchRollbackInfo(DataSource ds,String datasource,List<BranchRollbackInfo> info) {
		if(_txcResource.get() == null || !_txcResource.get().containsKey(ds)) {
			logger.error("分布式事务状态错误");
			throw new TxcTransactionStatusError("分布式事务状态错误 Expected Status is NOT NULL,But NULL");
		}
		BranchRollbackMessage message = (BranchRollbackMessage) _txcResource.get().get(ds);
		message.addBranchRollbackInfo(datasource,info);
	}
	
	protected static void addBranchRollbackInfo(DataSource ds,String datasource,BranchRollbackInfo info) {
		if(_txcResource.get() == null || !_txcResource.get().containsKey(ds)) {
			logger.error("分布式事务状态错误");
			throw new TxcTransactionStatusError("分布式事务状态错误 Expected Status is NOT NULL,But NULL");
		}
		BranchRollbackMessage message = (BranchRollbackMessage) _txcResource.get().get(ds);
		message.addBranchRollbackInfo(datasource,info);
	}
}

