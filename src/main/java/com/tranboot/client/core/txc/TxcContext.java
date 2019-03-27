package com.tranboot.client.core.txc;

import com.gb.soa.omp.transaction.response.TransactionInitResponse;
import com.tranboot.client.exception.TxcMQException;
import com.tranboot.client.exception.TxcRedisException;
import com.tranboot.client.exception.TxcTransactionException;
import com.tranboot.client.service.txc.TxcRedisService;
import com.tranboot.client.spring.ContextUtils;

public final class TxcContext {
	
	protected static final String TXC_XID = "TXC_XID";
	protected static final String TXC_START = "TXC_START";
	protected static final String TXC_TIMEOUT = "TXC_TIMEOUT";

	protected static void bind(Long xid,Long txcStart,int timeout) {
		TxcContextOperator.setContextParam(TXC_XID, xid);
		TxcContextOperator.setContextParam(TXC_START, txcStart);
		TxcContextOperator.setContextParam(TXC_TIMEOUT, timeout);
	}

	protected static void unbind() {
		TxcContextOperator.removeContext();
	}

	public static Long getCurrentXid() {
		return (Long) TxcContextOperator.getContextParam(TXC_XID);
	}
	
	public static Long getTxcStart() {
		return (Long) TxcContextOperator.getContextParam(TXC_START);
	}
	
	public static Integer getTxcTimeout() {
		return (Integer) TxcContextOperator.getContextParam(TXC_TIMEOUT);
	}
	
	protected static boolean inTxcTransaction() {
		if (getCurrentXid() != null) {
			return true;
		}
		return false;
	}
	
	/**
	 * @param methodName 入口方法名
	 * @param rollbackMode	回滚策略0:并行，1:串行
	 * @param timeout 分布式事务超时时间 (秒)
	 * 超时时间的值不要超过dubbo provider超时时间,即dubbo consumer认为超时后就不再允许provider的分支事务提交了。
	 * 比如一个服务的超时时间为3秒，那么调用方（最外事务）可能由于超时异常，
	 * 已经发出分布式事务回滚消息，执行分布式事务滚。此时如果timeout的值超过3秒，
	 * 就会导致dubbo provider本地分支事务和分布式事务状态不一致
	 * @return xid
	 */
	public static Long beginTxc(String methodName,int rollbackMode,int timeout) {
		if(TxcContext.getCurrentXid() != null) {
			throw new TxcTransactionException(String.format("方法【%s】已经存在分布式事务", methodName));
		}
		TransactionInitResponse rep = TxcMethodInterceptor.insertTransactionLogViaDubbo(ContextUtils.getServerIp(), ContextUtils.getSystemName(), methodName, rollbackMode);
//		TxcContext.bind(rep.getTransactionId(), rep.getTransactionStartTime(), timeout);
		try {
			TxcContext.bind(rep.getTransactionId(), ContextUtils.getBean(TxcRedisService.class).getCurrentTimeMillisFromRedis(), timeout);
		} catch (TxcRedisException e) {
			throw new TxcTransactionException("获取分布式事务开始时间失败!");
		}
		return rep.getTransactionId();
	}
	
	/**
	 * 默认分布式事务超时时间10秒
	 * @param methodName 入口方法名
	 * @param rollbackMode	回滚策略0:并行，1:串行
	 * @return xid
	 */
	public static Long beginTxc(String methodName,int rollbackMode) {
		return beginTxc(methodName, rollbackMode, 10);
	}
	
	/**
	 * 提交分布式事务，在最外层分支事务中调用
	 * @return
	 * @throws TxcMQException 
	 */
	public static boolean commitTxc() throws TxcMQException {
		Long xid = getCurrentXid();
		if(xid == null) {
			throw new TxcTransactionException("不存在分布式事务");
		}
		TxcMethodInterceptor.sendCommitMessage(xid);
		TxcContext.unbind();
		return true;
	}
	
	/**
	 * 回滚分布式事务，在最外层分支事务中调用
	 * @return
	 */
	public static boolean rollbackTxc() {
		Long xid = getCurrentXid();
		if(xid == null) {
			throw new TxcTransactionException("不存在分布式事务");
		}
		TxcMethodInterceptor.sendRollbackMessage(xid);
		TxcContext.unbind();
		return true;
	}
}

