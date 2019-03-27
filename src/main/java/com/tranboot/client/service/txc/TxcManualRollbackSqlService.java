package com.tranboot.client.service.txc;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TxcManualRollbackSqlService {
	
	protected Logger logger = LoggerFactory.getLogger(TxcManualRollbackSqlService.class);
	
	protected Map<String, Map<String, Object>> rollbackCache = new HashMap<>();
	
	public abstract void init();
	
	public Map<String, Object> map(String originalSql) {
		return rollbackCache.get(originalSql);
	}
}
