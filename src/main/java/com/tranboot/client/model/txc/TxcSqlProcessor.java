package com.tranboot.client.model.txc;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface TxcSqlProcessor {
	
	public static final Logger logger = LoggerFactory.getLogger(TxcSqlProcessor.class);
	
	public TxcSQL parse();
	
	public boolean manual();
	
	public boolean auto();
}
