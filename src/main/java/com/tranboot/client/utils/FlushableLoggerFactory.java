package com.tranboot.client.utils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedFNATP;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;

/**
 * 
 * 用来合并输出一个线程的所有日志
 * @author xuelong.chen
 *
 */
public class FlushableLoggerFactory {
	
	private static ThreadLocal<StringBuilder> cache = new ThreadLocal<>();
	
	private static final String SQLLOG_FILE = "/opt/sql.log";
	private static final String SQLLOG_FILE_PATTERN = "/opt/sql.%d{yyyy-MM-dd}_%i.log";
	private static final String LAYOUT_PATTERN = "%msg%n";
	private static final String MAX_LOG_FILE_SIZE = "50MB";
	
	public static FlushableLogger getLogger(Logger logger) {
		if(logger instanceof ch.qos.logback.classic.Logger) {
			ch.qos.logback.classic.Logger _logger = (ch.qos.logback.classic.Logger)logger;
			_logger.setAdditive(true);
			_logger.setLevel(Level.DEBUG);//FIXME
			RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
			appender.setContext(_logger.getLoggerContext());
			appender.setName("sqlLog");
			appender.setFile(SQLLOG_FILE);
			appender.setAppend(true);
			TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new TimeBasedRollingPolicy<>();
			rollingPolicy.setContext(_logger.getLoggerContext());
			rollingPolicy.setFileNamePattern(SQLLOG_FILE_PATTERN);
			rollingPolicy.setMaxHistory(3);
			rollingPolicy.setParent(appender);
			SizeAndTimeBasedFNATP<ILoggingEvent> timeBasedTriggering = new SizeAndTimeBasedFNATP<>();
			timeBasedTriggering.setMaxFileSize(FileSize.valueOf(MAX_LOG_FILE_SIZE));
			rollingPolicy.setTimeBasedFileNamingAndTriggeringPolicy(timeBasedTriggering);
			rollingPolicy.start();
			timeBasedTriggering.start();
			appender.setRollingPolicy(rollingPolicy);
			PatternLayoutEncoder encoder = new PatternLayoutEncoder();
			encoder.setPattern(LAYOUT_PATTERN);
			encoder.setContext(_logger.getLoggerContext());
			encoder.start();
			appender.setEncoder(encoder);
			appender.start();
			_logger.addAppender(appender);
		}else {
			logger.error("sqllog only support logback implements");
		}
		return (FlushableLogger) Proxy.newProxyInstance(FlushableLoggerFactory.class.getClassLoader(), new Class<?>[] {FlushableLogger.class}, new FlushableLoggerHandler(logger));
	};
	
	static class FlushableLoggerHandler implements InvocationHandler {

		private Logger delegator;
		
		public FlushableLoggerHandler(Logger logger) {
			this.delegator = logger;
		}
		
		public void cache(String log) {
			StringBuilder sbuilder = cache.get();
			if(sbuilder == null) {
				sbuilder = new StringBuilder();
				cache.set(sbuilder);
			}
			sbuilder.append(log).append(System.lineSeparator());
		}
		
		public void flush() {
			StringBuilder sbuilder = cache.get();
			if(sbuilder == null) return;
			delegator.info(StringUtils.substringBeforeLast(sbuilder.toString(), System.lineSeparator()));
			cache.remove();
		}
		
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			if(method.getName().equals("flush")) {
				flush();
				return null;
			}else if(method.getName().equals("cache")) {
				if(args.length == 1) {
					cache(args[0].toString());
					return null;
				}else {
					throw new UnsupportedOperationException("only support cache(String)");
				}
			}else {
				method.invoke(delegator, args);
				return null;
			}
		}
	}
	
	public interface FlushableLogger extends Logger{
		public void flush();
		public void cache(String log);
	}
	
	public static void main(String[] args) {
		PlatformTransactionManager manager = (PlatformTransactionManager) Proxy.newProxyInstance(FlushableLoggerFactory.class.getClassLoader(), new Class<?>[] {PlatformTransactionManager.class}, new InvocationHandler() {
			private DataSourceTransactionManager manager = new DataSourceTransactionManager();
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				return method.invoke(manager, args);
			}
		});
		manager.getTransaction(null);
	}
}
