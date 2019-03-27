package com.tranboot.client.spring;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;

import com.tranboot.client.core.txc.TxcTransactionScaner;

public class TxcModuleInitializeRegistrar implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		AnnotationAttributes attributes = AnnotationAttributes.fromMap(importingClassMetadata
		        .getAnnotationAttributes(EnableTxc.class.getName()));
		    String systemName = attributes.getString("systemName");
		    String txcMqNameServerAddr = attributes.getString("txcMqNameServerAddr");
		    int systemId = attributes.getNumber("systemId");
		    BeanDefinitionBuilder txcTransactionScanerBeanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(TxcTransactionScaner.class);
		    txcTransactionScanerBeanDefinitionBuilder.addPropertyValue("systemName", systemName);
		    txcTransactionScanerBeanDefinitionBuilder.addPropertyValue("systemId", systemId);
		    txcTransactionScanerBeanDefinitionBuilder.addPropertyValue("txcMqNameServerAddr", txcMqNameServerAddr);
		    registry.registerBeanDefinition(TxcTransactionScaner.class.getName(), txcTransactionScanerBeanDefinitionBuilder.getBeanDefinition());
	}

}
