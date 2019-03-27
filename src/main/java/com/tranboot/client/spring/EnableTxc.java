package com.tranboot.client.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Import(TxcModuleInitializeRegistrar.class)
public @interface EnableTxc {
	/**
	 * 应用名
	 * @return
	 */
	String systemName() default "default_system_name";
	/**
	 * 应用id
	 * @return
	 */
	int systemId() default 0;
	/**
	 * rocketmq name server 地址
	 * @return
	 */
	String txcMqNameServerAddr();
}
