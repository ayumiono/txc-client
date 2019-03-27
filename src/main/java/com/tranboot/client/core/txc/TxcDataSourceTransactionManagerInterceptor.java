package com.tranboot.client.core.txc;

import static com.tranboot.client.utils.MetricsReporter.beginTimer;
import static com.tranboot.client.utils.MetricsReporter.commitTimer;
import static com.tranboot.client.utils.MetricsReporter.rollbackTimer;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.AopInvocationException;
import org.springframework.aop.RawTargetAccess;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.codahale.metrics.Timer;
import com.tranboot.client.exception.TxcRedisException;
import com.tranboot.client.exception.TxcTransactionTimeoutException;
import com.tranboot.client.model.txc.BranchRollbackMessage;
import com.tranboot.client.service.txc.TxcMqService;
import com.tranboot.client.service.txc.TxcRedisService;
import com.tranboot.client.spring.ContextUtils;
import com.tranboot.client.utils.BranchIdGenerator;

/**
 * 
 * 分布式事务dsTransactionManager拦截器
 * 原则上：
 * 
 * 不能打破原本的commit,rollback语义
 * 
 * commit成功，不允许抛出任何错误，否则业务代码误操作可能导致原本成功的业务流程会走回滚流程
 * commit不成功，不允许吃掉任何错误，必须对外抛出异常，并通知最外层事务回滚
 * rollback同理
 * 
 * 
 * doBegin 设置threadLocal 
 * doCommit 将threadlocal 中的回滚sql存入redis 
 * doRollback 清空threadLocal
 * 
 * @author xuelong.chen
 *
 */
public class TxcDataSourceTransactionManagerInterceptor implements MethodInterceptor {
	
	private static final Logger logger = LoggerFactory.getLogger(TxcDataSourceTransactionManagerInterceptor.class);
	
	private TxcRedisService txcRedisService;
	
	private DataSourceTransactionManager target;
	
	
	public TxcDataSourceTransactionManagerInterceptor(DataSourceTransactionManager target,TxcRedisService txcRedisService) {
		this.target = target;
		this.txcRedisService = txcRedisService;
	}

	public Object invoke(Object proxy,Method method,Object[] args,MethodProxy methodProxy) throws Throwable {
		String name = method.getName();
		if ("doBegin".equals(name)) {
			Timer.Context context = beginTimer.time();
			try {
				if(TxcContext.getCurrentXid() != null) {
					logger.debug("txc分支事务开启...");
					long branchId = BranchIdGenerator.branchId();
					BranchRollbackMessage message = TxcRollbackSqlManagerV2.beginTransaction(target.getDataSource(),branchId, TxcContext.getCurrentXid(), ContextUtils.getServerIp());
					txcRedisService.hput(TxcRedisService.buildTransactionModel(message));
					Object retVal = methodProxy.invoke(this.target, args);
					logger.debug("txc分支事务开启成功");
					return processReturnType(proxy, this.target, method, retVal);
				}else {
					Object retVal = methodProxy.invoke(this.target, args);
					return processReturnType(proxy, this.target, method, retVal);
				}
			} finally {
				context.stop();
			}
		} else if ("doCommit".equals(name)) {
			Timer.Context context = commitTimer.time();
			if(TxcContext.getCurrentXid() != null) {
				logger.debug("txc分支事务提交...");
				try {
					long redisTime = txcRedisService.getCurrentTimeMillisFromRedis();
					if(redisTime > TxcContext.getTxcStart() + TxcContext.getTxcTimeout() * 1000) {
						logger.error("分布式事务超时,将自动rollback本地事务.");
						throw new TxcTransactionTimeoutException(String.format("分布式事务%d超时: 系统时间:%d 事务开始时间:%d 超时设置:%d", 
								TxcContext.getCurrentXid(),redisTime,TxcContext.getTxcStart(),TxcContext.getTxcTimeout() * 1000));
//						try {
//							this.target.rollback((TransactionStatus) args[0]);
//							try {
//								logger.debug("txc分支事务超时自动回滚...");
//								BranchRollbackMessage message = TxcRollbackSqlManagerV2.get(target.getDataSource());
//								message.setStatus(TxcRedisService.ROLLBACKABLE_STATUS);
//								logger.debug("更新reids状态位:{}",message);
//								txcRedisService.hput(TxcRedisService.buildTransactionModel(message));
//								logger.debug("更新reids状态位成功,txc分支事务超时自动回滚成功");
//							} catch (Exception e) {
//								logger.warn("更新分支事务redis状态出错！【本地事务已经超时自动回滚成功，不影响数据】", e);
//							}
//							return null;
//						} catch (Exception e) {
//							logger.error("本地分支事务超时自动回滚失败",e);
//							throw e;
//						} finally {
//							if(TxcContext.getCurrentXid() != null) {
//								TxcRollbackSqlManagerV2.releaseResource(target.getDataSource());
//							}
//							context.stop();
//						}
					}
				} catch (TxcRedisException e) {
					throw new TxcTransactionTimeoutException("获取redis 系统时间出错!");
				}
			}
			try {
				Object retVal = methodProxy.invoke(this.target, args);
				retVal = processReturnType(proxy, this.target, method, retVal);
				if(TxcContext.getCurrentXid() != null) {
					try {
						BranchRollbackMessage message = TxcRollbackSqlManagerV2.get(target.getDataSource());
						logger.debug("更新reids状态位:{}",message);
						message.setStatus(TxcRedisService.COMMITTABLE_STATUS);
						txcRedisService.hput(TxcRedisService.buildTransactionModel(message));
						logger.debug("更新reids状态位成功,txc分支事务提交成功");
					} catch (Exception e) {
						logger.warn("更新分支事务redis状态出错！【本地事务已经提交成功，不影响数据】", e);
					}
				}
				return retVal;
			} catch(Exception e) {
				logger.error("本地分支事务commit失败", e);
				throw e;
			} finally {
				if(TxcContext.getCurrentXid() != null) {
					TxcRollbackSqlManagerV2.releaseResource(target.getDataSource());
				}
				context.stop();
			}
		} else if ("doRollback".equals(name)) {
			Timer.Context context = rollbackTimer.time();
			try {
				Object retVal = methodProxy.invoke(this.target, args);
				if(TxcContext.getCurrentXid() != null) {
					try {
						logger.debug("txc分支事务回滚...");
						BranchRollbackMessage message = TxcRollbackSqlManagerV2.get(target.getDataSource());
						message.setStatus(TxcRedisService.ROLLBACKABLE_STATUS);
						logger.debug("更新reids状态位:{}",message);
						txcRedisService.hput(TxcRedisService.buildTransactionModel(message));
						logger.debug("更新reids状态位成功,txc分支事务回滚成功");
					} catch (Exception e) {
						logger.warn("更新分支事务redis状态出错！【本地事务已经回滚成功，不影响数据】",e);
					}
				}
				return processReturnType(proxy, this.target, method, retVal);
			}catch(Exception e) {
				logger.error("本地分支事务rollback失败！",e);
				throw e;
			} finally {
				if(TxcContext.getCurrentXid() != null) {
					TxcRollbackSqlManagerV2.releaseResource(target.getDataSource());
				}
				context.stop();
			}
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

