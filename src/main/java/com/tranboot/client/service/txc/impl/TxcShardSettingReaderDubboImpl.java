package com.tranboot.client.service.txc.impl;

import com.gb.soa.omp.transaction.request.SharedColumnGetRequest;
import com.gb.soa.omp.transaction.response.SharedColumnGetResponse;
import com.gb.soa.omp.transaction.service.TransactionInitService;
import com.tranboot.client.service.txc.TxcShardSettingReader;
import com.tranboot.client.spring.ContextUtils;

public class TxcShardSettingReaderDubboImpl implements TxcShardSettingReader {
	
	@Override
	public String shardFiled(String datasource, String table) {
		SharedColumnGetRequest request = new SharedColumnGetRequest();
		request.setSchema(datasource.toLowerCase());
		request.setTable(table.toLowerCase());
		SharedColumnGetResponse response = ContextUtils.getBean(TransactionInitService.class).getSharedColumn(request);
		if(response.getCode() != 0L) {
			return null;
		}
		return response.getSharedColumnName().toLowerCase();
	}

}
