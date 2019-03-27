package com.tranboot.client.spring;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

public class ContextUtils{
	
	private static ApplicationContext applicationContext;
	
	private static Map<String, Object> txcContext = new HashMap<>();
	
	public static final String BEAN_NAME_TXCREDISSERVICE = "txcRedisService";
	public static final String BEAN_NAME_TXCREDISTEMPLATE = "txcRedisTemplate";
	public static final String BEAN_NAME_TXCMQSERVICE = "txcMqService";
	public static final String BEAN_NAME_STRINGREDISTEMPLATE = "stringRedisTemplate";
	
	public static final String KEY_SERVER_IP = "serverIp";
	public static final String KEY_SYSTEM_ID = "systemId";
	public static final String KEY_SYSTEM_NAME = "systemName";

	public static synchronized void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		if(ContextUtils.applicationContext == null) {
			ContextUtils.applicationContext = applicationContext;
		}
	}
	
	public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

	public static Object getBean(String name) throws BeansException {
        return applicationContext.getBean(name);
    }
	
	public static <T> T getBean(Class<T> clazz) {
		return applicationContext.getBean(clazz);
	}
	
	public static <T> T getBean(String beanName,Class<T> clazz) {
		return applicationContext.getBean(beanName, clazz);
	}
	
	public static String getServerIp() {
		return txcContext.get(KEY_SERVER_IP).toString();
	}
	
	public static void setServerIp(String serverIp) {
		txcContext.put(KEY_SERVER_IP, serverIp);
	}
	
	public static int getSystemId() {
		return Integer.parseInt(txcContext.get(KEY_SYSTEM_ID).toString());
	}
	
	public static String getSystemName() {
		return txcContext.get(KEY_SYSTEM_NAME).toString();
	}
	
	public static void setSystemId(int systemId) {
		txcContext.put(KEY_SYSTEM_ID, systemId);
	}
	
	public static void setSystemName(String systemName) {
		txcContext.put(KEY_SYSTEM_NAME, systemName);
	}
	
	public static void addTxcContextParam(String key,Object value) {
		txcContext.put(key, value);
	}
	
	public static Object getTxcContextParam(String key) {
		return txcContext.get(key);
	}
}
