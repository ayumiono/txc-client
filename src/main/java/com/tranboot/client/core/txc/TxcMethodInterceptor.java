package com.tranboot.client.core.txc;

import static com.tranboot.client.utils.MetricsReporter.insertTxcLog;
import static com.tranboot.client.utils.MetricsReporter.sendCommitMessage;
import static com.tranboot.client.utils.MetricsReporter.sendRollbackMessage;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.gb.soa.omp.transaction.request.TransactionInitRequest;
import com.gb.soa.omp.transaction.response.TransactionInitResponse;
import com.gb.soa.omp.transaction.service.TransactionInitService;
import com.tranboot.client.exception.TxcMQException;
import com.tranboot.client.exception.TxcTransactionException;
import com.tranboot.client.service.txc.TxcMqService;
import com.tranboot.client.spring.ContextUtils;

/**
 * 基于注解方式
 * @author xuelong.chen
 */
public class TxcMethodInterceptor implements MethodInterceptor {
	
	private static final Logger logger = LoggerFactory.getLogger(TxcMethodInterceptor.class);
	
	
	protected static final TxcTransaction a = new DefaultTxcTransaction();
	
	protected static final AtomicReference<String> insertSql = new AtomicReference<String>();
	protected static final String dbtimeSql = "select SYSDATE() as dbtime";
	
	/**
	 * 保存每个方法对应分布式事务处理的元信息
	 */
	Map<Object, TxcTransaction> _context = new HashMap<>();

	public TxcMethodInterceptor(List<TxcMethodContext> contexts) {
		Iterator<TxcMethodContext> iterator = contexts.iterator();
		while (iterator.hasNext()) {
			TxcMethodContext local = iterator.next();
			_context.put(methodToStr(local.getLocalMethod()), local.getTxcTransaction());
		}
	}

	private static String methodToStr(Method method) {
		StringBuilder localStringBuilder = new StringBuilder();
		String str = method.getName();
		Class<?>[] arrayOfClass1 = method.getParameterTypes();
		localStringBuilder.append(str);
		localStringBuilder.append("(");
		int i = 0;
		for (Class<?> localClass : arrayOfClass1) {
			localStringBuilder.append(localClass.getName());
			i++;
			if (i >= arrayOfClass1.length)
				continue;
			localStringBuilder.append(",");
		}
		localStringBuilder.append(")");
		return localStringBuilder.toString();
	}

	public TxcTransaction findTxcTransaction(MethodInvocation invocation) {
		TxcTransaction localTxcTransaction = this._context.get(invocation.getMethod());
		if (localTxcTransaction == null) {
			synchronized (this) {
				localTxcTransaction = this._context.get(invocation.getMethod());
				if (localTxcTransaction == null) {
					String str = methodToStr(invocation.getMethod());
					localTxcTransaction = (TxcTransaction) this._context.get(str);
					if (localTxcTransaction == null)
						localTxcTransaction = a;
					Map<Object, TxcTransaction> localHashMap = new HashMap<Object, TxcTransaction>();
					localHashMap.putAll(this._context);
					localHashMap.remove(str);
					localHashMap.put(invocation.getMethod(), localTxcTransaction);
					this._context = localHashMap;
				}
			}
		}
		return localTxcTransaction;
	}
	
	protected static TransactionInitResponse insertTransactionLogViaDubbo(String ip_address,String systemName,String methodName,int rollbackMode) {
		TransactionInitRequest request = new TransactionInitRequest();
		request.setFromSystem(systemName);
		request.setIpAddress(ip_address);
		request.setMethodName(methodName);
		request.setTransactionRollbackFlag(Long.parseLong(rollbackMode+""));
		TransactionInitService dubboService = ContextUtils.getBean(TransactionInitService.class);
		TransactionInitResponse rep = dubboService.initTransaction(request);
		if(rep.getCode() != 0L || rep.getTransactionId() == null || rep.getTransactionStartTime() == null) {
			throw new TxcTransactionException("插入分布式事务数据库记录失败! "+rep.getMessage());
		}
		logger.debug(String.format("插入分布式事务数据库记录完成【%s】", rep.getTransactionId()+""));
		return rep;
	}
	
	@Deprecated
	protected static long insertTransactionLog(long xid,String ip_address,String systemName,String methodName,int rollbackMode) {
		Connection con = null;
		Timer.Context context = insertTxcLog.time();
		try {
			DataSource platformDs = ContextUtils.getBean("platformDataSource", DataSource.class);
			con = platformDs.getConnection();
			PreparedStatement ps = con.prepareStatement(dbtimeSql);
			ResultSet rs = ps.executeQuery();
			rs.next();
			Timestamp dbtime = rs.getTimestamp("dbtime");
			ps.close();
			
			if(insertSql.get() == null) {
				StringBuilder tmp = new StringBuilder();
				tmp.append("insert into transaction_log");
				tmp.append("(");
				tmp.append("transaction_id,");
				tmp.append("start_dtme,");
				tmp.append("ip_address,");
				tmp.append("transaction_state,");
				tmp.append("from_system,");
				tmp.append("method_name,");
				tmp.append("transaction_rollback_flag,");
				tmp.append("transaction_sign");
				tmp.append(")");
				tmp.append("values (");
				tmp.append("?,").append("?,").append("?,").append("?,").append("?,").append("?,").append("?,").append("?");
				tmp.append(")");
				insertSql.compareAndSet(null, tmp.toString());
			}
			
			ps = con.prepareStatement(insertSql.get());
			ps.setLong(1, xid);
			ps.setTimestamp(2, dbtime);
			ps.setString(3, ip_address);
			ps.setInt(4, 0);
			ps.setString(5, systemName);
			ps.setString(6, methodName);
			ps.setInt(7, rollbackMode);
			ps.setString(8, "N");
			int row = ps.executeUpdate();
			if(row != 1) {
				logger.error("txc事务表插入失败");
				throw new TxcTransactionException("txc事务表插入失败");
			}
			logger.debug("txc事务表插入完成,xid={}",xid);
			return dbtime.getTime();
		} catch (Exception e) {
			logger.error("txc事务表插入失败", e);
			throw new TxcTransactionException(e, "txc事务表插入失败");
		} finally {
			try {
				if(con != null) {
					con.close();
				}
				context.stop();
			} catch (SQLException e) {
				logger.error("手动释放连接失败", e);
				context.stop();
				throw new TxcTransactionException(e, "释放连接失败");
			}
		}
	}
	
	protected static void sendCommitMessage(long xid) throws TxcMQException {
		Timer.Context context = sendCommitMessage.time();
		TxcMqService mqService = ContextUtils.getBean(TxcMqService.class);
		try {
			mqService.sendCommit(xid, new Date());
		} catch (Exception e) {
			logger.error("发送txc提交消息失败。", e);
			throw new TxcMQException(e, "发送txc提交消息失败。【注意:后台默认会执行定时回滚任务，数据会被回滚掉，请捕获该异常，通知上游业务失败】");
		} finally {
			context.stop();
		}
	}
	
	protected static void sendRollbackMessage(long xid) {
		Timer.Context context = sendRollbackMessage.time();
		TxcMqService mqService = ContextUtils.getBean(TxcMqService.class);
		try {
			mqService.sendRollback(xid, new Date());
		} catch (Exception e) {
			logger.error("发送txc回滚消息失败。【注意:后台默认会执行定时回滚任务，数据会被回滚掉】", e);
		} finally {
			context.stop();
		}
	}
	
//	protected static void updateTransactionLogStatus(long xid,int status) {
//		try {
//			StringBuilder sbuilder = new StringBuilder();
//			sbuilder.append("update ");
//			sbuilder.append(ContextUtils.getTransactionLogTable());
//			sbuilder.append(" set ");
//			sbuilder.append("end_dtme=?,");
//			sbuilder.append("transaction_state=?,");
//			sbuilder.append("transaction_sign=? ");
//			sbuilder.append("where transaction_id=?");
//			String sql = sbuilder.toString();
//			DataSource platformDs = ContextUtils.getBean("platformDataSource", DataSource.class);
//			Connection con = platformDs.getConnection();
//			PreparedStatement ps = con.prepareStatement(sql);
//			ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
//			ps.setInt(2, status);
//			ps.setString(3, status == 1 ? "Y" : (status == 2 ? "N" : "N"));
//			ps.setLong(4, xid);
//			int row = ps.executeUpdate();
//			if(row != 1) {
//				logger.error("txc事务{}状态更新失败",xid);
//				throw new TxcTransactionException("txc事务表状态更新失败");
//			}
//			logger.debug("txc事务{}状态更新成功",xid);
//		} catch (Exception e) {
//			logger.error("txc事务表状态更新失败", e);
//			throw new TxcTransactionException(e, "txc事务表状态更新失败");
//		}
//	}
	
//	不要删除	不要删除	不要删除	不要删除	不要删除	不要删除	不要删除	不要删除	不要删除	不要删除
//	/* 
//	 * 在这里根据TxcTransaction元信息进行业务处理
//	 * 最外层事务自动托管:存在一种情况：原业务代不包事务，且有多条数据库更新操作
//	 * (non-Javadoc)
//	 * @see org.aopalliance.intercept.MethodInterceptor#invoke(org.aopalliance.intercept.MethodInvocation)
//	 */
//	@Override
//	public Object invoke(MethodInvocation invocation) throws Throwable {
//		TxcTransaction localTxcTransaction = findTxcTransaction(invocation);
//		if(localTxcTransaction != a) {
//			String transactionManagerBeanName = localTxcTransaction.transactionManagerBeanName();
//			DataSourceTransactionManager transactionManager;
//			if(transactionManagerBeanName.equals("")) {
//				transactionManager = ContextUtils.getBean(DataSourceTransactionManager.class);
//			}else {
//				transactionManager = ContextUtils.getBean(transactionManagerBeanName, DataSourceTransactionManager.class);
//			}
//			long xid = TxcIdGenerator.xid(ContextUtils.getSystemName(),ContextUtils.getSystemId(),System.currentTimeMillis(),UUID.randomUUID().toString());// 生成xid
//			TxcContext.bind(xid);
//			TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());
//			int rollbackMode = localTxcTransaction.rollbackMode().getMode();
//			String name = invocation.getMethod().getName();// 方法名method_name
//			//插表
//			insertTransactionLog(xid, ContextUtils.getServerIp(), ContextUtils.getSystemName(), name, rollbackMode);
//			Object result = null;
//			try {
//				result = invocation.proceed();
//			} catch (RuntimeException e) {
//				//最外层事务结束 发更新分布式事务状态消息,分布式事务全部成功，由最外层事务来决定
//				//分布式事务失败则可由每个分支事务来决定,分布式事务失败的消息放在datasource transaction manager interceptor中来做
//				transactionManager.rollback(status);
//				updateTransactionLogStatus(TxcContext.getCurrentXid(),2);
//				sendRollbackMessage(TxcContext.getCurrentXid());
//				TxcContext.unbind();
//				throw e;
//			}
//			//如果最外层没有事务，或最外层事务成功，才发分布式事务全部成功消息
//			updateTransactionLogStatus(TxcContext.getCurrentXid(),1);
//			sendCommitMessage(TxcContext.getCurrentXid());
//			TxcContext.unbind();
//			transactionManager.commit(status);
//			return result;
//		}
//		return invocation.proceed();
//	}

	/* 
	 * 如果能保证业务代码能够
	 * 遵循一般注解事务范式
	 * 方法开始 begin
	 * 方法结束 commit
	 * RuntimeException rollback
	 * 则完全可以全部基于注解实现分布式事务
	 */
	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		TxcTransaction localTxcTransaction = findTxcTransaction(invocation);
		if(localTxcTransaction != a) {
			int rollbackMode = localTxcTransaction.rollbackMode().getMode();
			int timeout = localTxcTransaction.timeout();
			String name = invocation.getMethod().getName();
			if(TxcContext.getCurrentXid() != null) {
				throw new TxcTransactionException(String.format("方法【%s】已经存在分布式事务", name));
			}
			TransactionInitResponse rep = insertTransactionLogViaDubbo(ContextUtils.getServerIp(), ContextUtils.getSystemName(), name, rollbackMode);
			TxcContext.bind(rep.getTransactionId(), rep.getTransactionStartTime(), timeout);
			Object result = null;
			try {
				result = invocation.proceed();
				TxcContext.commitTxc();
				return result;
			} catch (RuntimeException e) {
				TxcContext.rollbackTxc();
				throw e;
			}
		}else {
			return invocation.proceed();
		}
	}
}

