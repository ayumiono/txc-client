package com.tranboot.client.core.txc;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.LinkedList;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.TargetSource;
import org.springframework.aop.aspectj.AspectJExpressionPointcut;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.framework.autoproxy.AbstractAutoProxyCreator;
import org.springframework.aop.support.AopUtils;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.aop.target.SingletonTargetSource;
import org.springframework.beans.BeansException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.alibaba.dubbo.config.spring.ReferenceBean;
import com.gb.soa.omp.transaction.model.RedisTransactionModel;
import com.gb.soa.omp.transaction.service.TransactionInitService;
import com.tranboot.client.core.JdbcTemplatePointCut;
import com.tranboot.client.druid.util.JdbcUtils;
import com.tranboot.client.exception.TxcTransactionException;
import com.tranboot.client.model.DBType;
import com.tranboot.client.service.txc.TxcManualRollbackSqlService;
import com.tranboot.client.service.txc.TxcMqService;
import com.tranboot.client.service.txc.TxcRedisService;
import com.tranboot.client.service.txc.TxcShardSettingReader;
import com.tranboot.client.service.txc.impl.TxcManualRollbackSQLServiceXMLImpl;
import com.tranboot.client.service.txc.impl.TxcManualRollbackSqlServiceNullImpl;
import com.tranboot.client.service.txc.impl.TxcShardSettingReaderDubboImpl;
import com.tranboot.client.spring.ContextUtils;
import com.tranboot.client.utils.CustomAopProxy;

/**
 * 分布式事务启动入口
 * @author xuelong.chen
 */
public class TxcTransactionScaner extends AbstractAutoProxyCreator implements InitializingBean,ApplicationContextAware,BeanDefinitionRegistryPostProcessor {

	private String systemName;
	
	private Integer systemId;
	
	private String txcMqNameServerAddr;
	
	private String manualSql;
	
	private TxcMethodInterceptor interceptor;
	
	private static final long serialVersionUID = -6378434453422374519L;
	
	private void registryTxcMqService(BeanDefinitionRegistry registry) {
		BeanDefinitionBuilder bdbuilder = BeanDefinitionBuilder.genericBeanDefinition(TxcMqService.class);
		bdbuilder.addPropertyValue("nameServerAddr", txcMqNameServerAddr);
		registry.registerBeanDefinition(ContextUtils.BEAN_NAME_TXCMQSERVICE, bdbuilder.getBeanDefinition());
	}
	
	private void registryTxcManualRollbackSqlService(ConfigurableListableBeanFactory beanFactory) {
		if(manualSql == null) {
			beanFactory.registerSingleton(TxcManualRollbackSqlService.class.getName(), new TxcManualRollbackSqlServiceNullImpl()); ;
		}else {
			beanFactory.registerSingleton(TxcManualRollbackSqlService.class.getName(),new TxcManualRollbackSQLServiceXMLImpl(manualSql));
		}
	}
	
	private void registryTxcShardSettingReader(ConfigurableListableBeanFactory beanFactory) {
		beanFactory.registerSingleton(TxcShardSettingReader.class.getName(), new TxcShardSettingReaderDubboImpl());
	}
	
	@Override
	protected Object[] getAdvicesAndAdvisorsForBean(Class<?> beanClass, String beanName,
			TargetSource customTargetSource) throws BeansException {
		return new Object[] {interceptor};
	}
	
	//需要返回true，使用cglib代理。否则会出现类型转换错误
	@Override
	protected boolean shouldProxyTargetClass(Class<?> beanClass, String beanName) {
		return true;
	}
	
	@Override
	public void customizeProxyFactory(ProxyFactory proxyFactory) {
		proxyFactory.setFrozen(true);
		this.setFrozen(true);
		proxyFactory.setExposeProxy(true);
	}

	@Override
	protected Object wrapIfNecessary(Object bean, String beanName, Object cacheKey) {

		Class<?> localClass = bean.getClass();
		if(JdbcTemplate.class.isAssignableFrom(localClass)) {//jdbctemplate拦截器
			DataSource ds = ((JdbcTemplate) bean).getDataSource();
			String datasource = null;
			DBType dbType = DBType.MYSQL;
			
			try {
				if(Class.forName("com.alibaba.druid.pool.DruidAbstractDataSource").isAssignableFrom(ds.getClass())) {
					Method getUrl = Class.forName("com.alibaba.druid.pool.DruidAbstractDataSource").getMethod("getUrl");
					String driverUrl = (String) getUrl.invoke(ds);
					datasource = extractDbName(driverUrl);
					Method getDriverClassName = Class.forName("com.alibaba.druid.pool.DruidAbstractDataSource").getMethod("getDriverClassName");
					String driverClass = (String) getDriverClassName.invoke(ds);
					if(driverClass.equals(JdbcUtils.ORACLE_DRIVER) || driverClass.equals(JdbcUtils.ORACLE_DRIVER2)) {
						dbType = DBType.ORACLE;
					}
				}else {
					logger.warn("连接池没有使用druid,无法提取出数据库名、数据库类型");
				}
			} catch (Exception e) {
				throw new TxcTransactionException(e, "txc生成jdbctemplate拦截对象失败");
			}
			
			AspectJExpressionPointcut pointcut1 = new AspectJExpressionPointcut();
			pointcut1.setExpression(JdbcTemplatePointCut.update1);
			DefaultPointcutAdvisor advisor1 = new DefaultPointcutAdvisor(pointcut1, new TxcJdbcTemplateInterceptor1(datasource,ds,ContextUtils.getBean(TxcRedisService.class),dbType));
			AspectJExpressionPointcut pointcut2 = new AspectJExpressionPointcut();
			pointcut2.setExpression(JdbcTemplatePointCut.update2);
			DefaultPointcutAdvisor advisor2 = new DefaultPointcutAdvisor(pointcut2, new TxcJdbcTemplateInterceptor2(datasource,ds,ContextUtils.getBean(TxcRedisService.class),dbType));
			AspectJExpressionPointcut pointcut3 = new AspectJExpressionPointcut();
			pointcut3.setExpression(JdbcTemplatePointCut.batchUpdate1);
			DefaultPointcutAdvisor advisor3 = new DefaultPointcutAdvisor(pointcut3, new TxcJdbcTemplateInterceptor3(datasource,ds,ContextUtils.getBean(TxcRedisService.class),dbType));
			AspectJExpressionPointcut pointcut4 = new AspectJExpressionPointcut();
			pointcut4.setExpression(JdbcTemplatePointCut.batchUpdate2);
			DefaultPointcutAdvisor advisor4 = new DefaultPointcutAdvisor(pointcut4, new TxcJdbcTemplateInterceptor4(datasource,ds,ContextUtils.getBean(TxcRedisService.class),dbType));
			bean = createProxy(localClass, beanName, new Object[] {advisor1,advisor2,advisor3,advisor4}, new SingletonTargetSource(bean));
			return bean;
		}
		
		//cglib不能代理final方法，调用getTransaction时会报错
		if(PlatformTransactionManager.class.isAssignableFrom(localClass)) {
			try {
				return CustomAopProxy.proxy(CustomAopProxy.getTargetClass(bean), new TxcDataSourceTransactionManagerInterceptor((DataSourceTransactionManager)bean,ContextUtils.getBean(TxcRedisService.class)));
			}catch (Exception e) {
				throw new TxcTransactionException(e, "txc生成transactionManager拦截对象失败");
			}
		}
		
		Method[] arrayOfMethod = localClass.getMethods();
		LinkedList<TxcMethodContext> localLinkedList = new LinkedList<TxcMethodContext>();
		for (Method localMethod : arrayOfMethod) {
			TxcTransaction localTxcTransaction = (TxcTransaction) localMethod.getAnnotation(TxcTransaction.class);
			if (localTxcTransaction == null)
				continue;
			localLinkedList.add(new TxcMethodContext(localTxcTransaction, localMethod));
		}
		if(localLinkedList.size() != 0) {
			TxcMethodInterceptor i = new TxcMethodInterceptor(localLinkedList);
			this.interceptor = i;
		}else {
			return bean;
		}
		if(!AopUtils.isAopProxy(bean)) {
			bean = super.wrapIfNecessary(bean, beanName, cacheKey);
		}else {
			bean = createProxy(localClass, beanName, getAdvicesAndAdvisorsForBean(null,null,null), new SingletonTargetSource(bean));
		}
		return bean;
	}

	public String getSystemName() {
		return systemName;
	}

	public void setSystemName(String systemName) {
		this.systemName = systemName;
	}

	public Integer getSystemId() {
		return systemId;
	}

	public void setSystemId(Integer systemId) {
		this.systemId = systemId;
	}
	
	public String getTxcMqNameServerAddr() {
		return txcMqNameServerAddr;
	}

	public void setTxcMqNameServerAddr(String txcMqNameServerAddr) {
		this.txcMqNameServerAddr = txcMqNameServerAddr;
	}

	public String getManualSql() {
		return manualSql;
	}

	public void setManualSql(String manualSql) {
		this.manualSql = manualSql;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if(StringUtils.isBlank(this.txcMqNameServerAddr)) {
			throw new Exception("txcMqNameServerAddr cannot be null");
		}
		if(StringUtils.isBlank(this.systemName)) {
			throw new Exception("txc systemName cannot be null");
		}
		if(this.systemId == null) {
			throw new Exception("txc systemId cannot be null");
		}
		ContextUtils.setServerIp(InetAddress.getLocalHost().getHostAddress());
		ContextUtils.setSystemId(systemId);
		ContextUtils.setSystemName(systemName);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		ContextUtils.setApplicationContext(applicationContext);
	}
	
	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		registryTxcManualRollbackSqlService(beanFactory);
		registryTxcShardSettingReader(beanFactory);
	}
	
	@Override
	public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
		
		String[] candidates = registry.getBeanDefinitionNames();
		boolean transactionInitServiceRegistried = false;
		boolean jedisConnectionFactoryRegistried = false;
		String jedisConnectionFactoryBeanName = null;
		String stringRedisTemplateBeanName = null;
		String stringRedisSerializerBeanName = null;
		for(String candidate : candidates) {
			BeanDefinition bd = registry.getBeanDefinition(candidate);
			//spring boot
			if(candidate.equals("redisConnectionFactory")) {
				jedisConnectionFactoryBeanName = candidate;
				jedisConnectionFactoryRegistried = true;
				continue;
			}
			if(candidate.equals("stringRedisTemplate")) {
				stringRedisTemplateBeanName = candidate;
				continue;
			}
			if(bd.getBeanClassName() == null) {
				continue;
			}
			if(bd.getBeanClassName().equals(ReferenceBean.class.getName())) {//dubbo
				MutablePropertyValues propertyValue = registry.getBeanDefinition(candidate).getPropertyValues();
				String dubbo_interface = propertyValue.get("interface").toString();
				if(dubbo_interface.equals(TransactionInitService.class.getName())) {
					transactionInitServiceRegistried = true;
				}
			}
			if(bd.getBeanClassName().equals(JedisConnectionFactory.class.getName())) {
				jedisConnectionFactoryBeanName = candidate;
				jedisConnectionFactoryRegistried = true;
			}
			if(bd.getBeanClassName().equals(StringRedisTemplate.class.getName())) {//stringRedisTemplate
				stringRedisTemplateBeanName = candidate;
			}
			if(bd.getBeanClassName().equals(StringRedisSerializer.class.getName())) {//stringRedisSerializer
				stringRedisSerializerBeanName = candidate;
			}
		}
		if(!transactionInitServiceRegistried) {
			throw new NoSuchBeanDefinitionException("TransactionInitService");
		}
		if(!jedisConnectionFactoryRegistried) {
			throw new NoSuchBeanDefinitionException("JedisConnectionFactory");
		}
		if(stringRedisTemplateBeanName == null) {
			BeanDefinitionBuilder stringRedisTemplateBeanBuilder = BeanDefinitionBuilder.genericBeanDefinition(StringRedisTemplate.class);
			stringRedisTemplateBeanBuilder.addConstructorArgReference(jedisConnectionFactoryBeanName);
			stringRedisTemplateBeanName = ContextUtils.BEAN_NAME_STRINGREDISTEMPLATE;
			registry.registerBeanDefinition(stringRedisTemplateBeanName, stringRedisTemplateBeanBuilder.getBeanDefinition());
		}
		
		BeanDefinitionBuilder txcRedisTemplateBeanBuilder = BeanDefinitionBuilder.genericBeanDefinition(RedisTemplate.class);
		txcRedisTemplateBeanBuilder.addPropertyReference("connectionFactory", jedisConnectionFactoryBeanName);
		if(stringRedisSerializerBeanName != null) {
			txcRedisTemplateBeanBuilder.addPropertyReference("keySerializer", stringRedisSerializerBeanName);
			txcRedisTemplateBeanBuilder.addPropertyReference("hashKeySerializer", stringRedisSerializerBeanName);
		}else {
			txcRedisTemplateBeanBuilder.addPropertyValue("keySerializer", new StringRedisSerializer());
			txcRedisTemplateBeanBuilder.addPropertyValue("hashKeySerializer", new StringRedisSerializer());
		}
		RedisSerializer<RedisTransactionModel> valueSer = new Jackson2JsonRedisSerializer<>(RedisTransactionModel.class);
		txcRedisTemplateBeanBuilder.addPropertyValue("valueSerializer", valueSer);
		txcRedisTemplateBeanBuilder.addPropertyValue("hashValueSerializer", valueSer);
		registry.registerBeanDefinition(ContextUtils.BEAN_NAME_TXCREDISTEMPLATE, txcRedisTemplateBeanBuilder.getBeanDefinition());
		BeanDefinitionBuilder txcRedisServiceBeanBuilder = BeanDefinitionBuilder.genericBeanDefinition(TxcRedisService.class);
		txcRedisServiceBeanBuilder.addConstructorArgReference(ContextUtils.BEAN_NAME_TXCREDISTEMPLATE);
		txcRedisServiceBeanBuilder.addConstructorArgReference(stringRedisTemplateBeanName);
		registry.registerBeanDefinition(ContextUtils.BEAN_NAME_TXCREDISSERVICE, txcRedisServiceBeanBuilder.getBeanDefinition());
		registryTxcMqService(registry);
	}
	
	private String extractDbName(String jdbcUrl) {
		try {
			String datasource = StringUtils.substringBefore(StringUtils.substringAfterLast(jdbcUrl, "/"),"?");
			return datasource;
		} catch (Exception e) {
			logger.error("提取数据库名失败"+e.getMessage());
		}
		return null;
	}
}

