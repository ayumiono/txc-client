package com.tranboot.client.core.txc;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.tranboot.client.model.txc.TxcRollbackMode;

/**
 * 分布式事务元信息
 * @author xuelong.chen
 */
@Documented
@Retention(RUNTIME)
@Target(METHOD)
public @interface TxcTransaction {
	
	/**
	 * 分布式事务回滚策略 0并行回滚 1串行回滚
	 * @return
	 */
	public TxcRollbackMode rollbackMode();

	/**
	 * 分布式事务超时时间(秒)
	 * @return
	 */
	public int timeout() default 120;
}
