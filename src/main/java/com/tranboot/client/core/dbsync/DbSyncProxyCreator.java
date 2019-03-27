package com.tranboot.client.core.dbsync;

import java.util.Set;

import javax.sql.DataSource;

import org.springframework.aop.TargetSource;
import org.springframework.aop.aspectj.AspectJExpressionPointcut;
import org.springframework.aop.framework.autoproxy.AbstractAutoProxyCreator;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.aop.target.SingletonTargetSource;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.gb.soa.omp.dbsync.model.RdisSyncModel;
import com.tranboot.client.core.JdbcTemplatePointCut;
import com.tranboot.client.service.dbsync.DbSyncTaskBuilder;
import com.tranboot.client.service.dbsync.DbsyncMqService;
import com.tranboot.client.service.dbsync.RedisService;
import com.tranboot.client.spring.ContextUtils;
import com.tranboot.client.utils.CustomAopProxy;

/**
 * 
 * 数据库双写配置bean
 * @author xuelong.chen
 *
 */
public class DbSyncProxyCreator extends AbstractAutoProxyCreator implements ApplicationContextAware{

	private static final long serialVersionUID = 5105358253096092417L;
	
	private String dbsyncMqNameServerAddr;
	
//	private Set<String> proxyTargetJdbcTemplates;
//	private Set<String> proxyTargetDsTransactionManager;
	
	private Set<DataSource> proxyTargetDs;
	
	DbSyncTaskBuilder taskBuilder;
	
	private DbsyncMqService initDbsyncMqService() {
		return new DbsyncMqService(dbsyncMqNameServerAddr);
	}
	
	// 需要返回true，否则会出现类型转换错误
	@Override
	protected boolean shouldProxyTargetClass(Class<?> beanClass, String beanName) {
		return true;
	}

	@Override
	protected Object wrapIfNecessary(Object bean, String beanName, Object cacheKey) {
		Class<?> localClass = bean.getClass();
		if (JdbcTemplate.class.isAssignableFrom(localClass)) {
			if(!proxyTargetDs.contains(((JdbcTemplate) bean).getDataSource())) {
				return bean;
			}
			AspectJExpressionPointcut pointcut1 = new AspectJExpressionPointcut();
			pointcut1.setExpression(JdbcTemplatePointCut.update1);
			DefaultPointcutAdvisor advisor1 = new DefaultPointcutAdvisor(pointcut1, new DbSyncJdbcTemplateInterceptor1(taskBuilder));
			AspectJExpressionPointcut pointcut2 = new AspectJExpressionPointcut();
			pointcut2.setExpression(JdbcTemplatePointCut.update2);
			DefaultPointcutAdvisor advisor2 = new DefaultPointcutAdvisor(pointcut2, new DbSyncJdbcTemplateInterceptor2(taskBuilder));
			AspectJExpressionPointcut pointcut3 = new AspectJExpressionPointcut();
			pointcut3.setExpression(JdbcTemplatePointCut.batchUpdate1);
			DefaultPointcutAdvisor advisor3 = new DefaultPointcutAdvisor(pointcut3, new DbSyncJdbcTemplateInterceptor3(taskBuilder));
			AspectJExpressionPointcut pointcut4 = new AspectJExpressionPointcut();
			pointcut4.setExpression(JdbcTemplatePointCut.batchUpdate2);
			DefaultPointcutAdvisor advisor4 = new DefaultPointcutAdvisor(pointcut4, new DbSyncJdbcTemplateInterceptor4(taskBuilder));
	//		bean = createProxy(localClass, beanName, new Object[] {new DbSyncJdbcTemplateInterceptor(taskBuilder)}, new SingletonTargetSource(bean));
			bean = createProxy(localClass, beanName, new Object[] {advisor1,advisor2,advisor3,advisor4}, new SingletonTargetSource(bean));
			return bean;
		}
		if(PlatformTransactionManager.class.isAssignableFrom(localClass)) {
			if(!proxyTargetDs.contains(((DataSourceTransactionManager) bean).getDataSource())) {
				return bean;
			}
			return CustomAopProxy.proxy(CustomAopProxy.getTargetClass(bean), new DbSyncDataSourceTransactionManagerInterceptor((DataSourceTransactionManager)bean,taskBuilder));
		}
//		if(proxyTargetJdbcTemplates.contains(beanName)) {
//			AspectJExpressionPointcut pointcut1 = new AspectJExpressionPointcut();
//			pointcut1.setExpression(JdbcTemplatePointCut.update1);
//			DefaultPointcutAdvisor advisor1 = new DefaultPointcutAdvisor(pointcut1, new DbSyncJdbcTemplateInterceptor1(taskBuilder));
//			AspectJExpressionPointcut pointcut2 = new AspectJExpressionPointcut();
//			pointcut2.setExpression(JdbcTemplatePointCut.update2);
//			DefaultPointcutAdvisor advisor2 = new DefaultPointcutAdvisor(pointcut2, new DbSyncJdbcTemplateInterceptor2(taskBuilder));
//			AspectJExpressionPointcut pointcut3 = new AspectJExpressionPointcut();
//			pointcut3.setExpression(JdbcTemplatePointCut.batchUpdate1);
//			DefaultPointcutAdvisor advisor3 = new DefaultPointcutAdvisor(pointcut3, new DbSyncJdbcTemplateInterceptor3(taskBuilder));
//			AspectJExpressionPointcut pointcut4 = new AspectJExpressionPointcut();
//			pointcut4.setExpression(JdbcTemplatePointCut.batchUpdate2);
//			DefaultPointcutAdvisor advisor4 = new DefaultPointcutAdvisor(pointcut4, new DbSyncJdbcTemplateInterceptor4(taskBuilder));
//			bean = createProxy(localClass, beanName, new Object[] {advisor1,advisor2,advisor3,advisor4}, new SingletonTargetSource(bean));
//			return bean;
//		}
//		if(proxyTargetDsTransactionManager.contains(beanName)) {
//			return CustomAopProxy.proxy(bean, new DbSyncDataSourceTransactionManagerInterceptor((DataSourceTransactionManager)bean,taskBuilder));
//		}
		return bean;
	}

	@Override
	protected Object[] getAdvicesAndAdvisorsForBean(Class<?> beanClass, String beanName,
			TargetSource customTargetSource) throws BeansException {
		return null;
	}

	public String getDbsyncMqNameServerAddr() {
		return dbsyncMqNameServerAddr;
	}

	public void setDbsyncMqNameServerAddr(String dbsyncMqNameServerAddr) {
		this.dbsyncMqNameServerAddr = dbsyncMqNameServerAddr;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		JedisConnectionFactory jedisConnectionFactory = applicationContext.getBean(JedisConnectionFactory.class);
		if(jedisConnectionFactory == null) {
			throw new RuntimeException("数据双写需要依赖redis，请往spring ioc容器中添加jedisConnectionFactory实例");
		}
		RedisTemplate<String, RdisSyncModel> template = new RedisTemplate<>();
		template.setConnectionFactory(jedisConnectionFactory);
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new Jackson2JsonRedisSerializer<>(RdisSyncModel.class));
		template.afterPropertiesSet();
		this.taskBuilder = new DbSyncTaskBuilder(initDbsyncMqService(),new RedisService(template));
		ContextUtils.setApplicationContext(applicationContext);
	}
	
//	public Set<String> getProxyTargetJdbcTemplates() {
//		return proxyTargetJdbcTemplates;
//	}
//
//	public void setProxyTargetJdbcTemplates(Set<String> proxyTargetJdbcTemplates) {
//		this.proxyTargetJdbcTemplates = proxyTargetJdbcTemplates;
//	}
//
//	public Set<String> getProxyTargetDsTransactionManager() {
//		return proxyTargetDsTransactionManager;
//	}
//
//	public void setProxyTargetDsTransactionManager(Set<String> proxyTargetDsTransactionManager) {
//		this.proxyTargetDsTransactionManager = proxyTargetDsTransactionManager;
//	}

	public Set<DataSource> getProxyTargetDs() {
		return proxyTargetDs;
	}

	public void setProxyTargetDs(Set<DataSource> proxyTargetDs) {
		this.proxyTargetDs = proxyTargetDs;
	}
}
