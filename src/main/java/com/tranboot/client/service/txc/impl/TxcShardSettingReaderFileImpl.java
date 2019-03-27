package com.tranboot.client.service.txc.impl;

import java.io.IOException;
import java.util.Properties;

import com.tranboot.client.exception.TxcTransactionException;
import com.tranboot.client.service.txc.TxcShardSettingReader;

public class TxcShardSettingReaderFileImpl implements TxcShardSettingReader {
	
	Properties setting;
	
	public TxcShardSettingReaderFileImpl() {
		setting = new Properties();
		try {
			setting.load(this.getClass().getClassLoader().getResourceAsStream("txc-shard-setting.properties"));
		} catch (IOException e) {
			throw new TxcTransactionException(e,"读取txc-shard-setting.properties出错");
		}
	}
	
	@Override
	public String shardFiled(String datasource, String table) {
		return setting.getProperty(String.format("%s-%s", datasource.toLowerCase(),table.toLowerCase()));
	}

}
