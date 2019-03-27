package com.tranboot.client.service.txc.impl;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.tranboot.client.exception.TxcTransactionException;
import com.tranboot.client.service.txc.TxcManualRollbackSqlService;

public class TxcManualRollbackSQLServiceXMLImpl extends TxcManualRollbackSqlService {
	
	public TxcManualRollbackSQLServiceXMLImpl(String xmlFile) {
		this.xmlFile = xmlFile;
		init();
	}
	
	private String xmlFile;
	
	@Override
	public void init() {
		try {
			parseXML();
		} catch (Exception e) {
			throw new TxcTransactionException(e, e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private void parseXML() throws Exception {
		logger.debug("开始解析{}配置",xmlFile);
		SAXReader reader = new SAXReader();
		InputStream is = this.getClass().getClassLoader().getResourceAsStream(xmlFile);
		if(is == null) throw new Exception("classpath下没有找到数据库手动回滚配置文件"+xmlFile);
		Document doc = reader.read(is);
		Element root = doc.getRootElement();
		Iterator<Element> iterator = root.elementIterator();
		while(iterator.hasNext()) {
			Element row = iterator.next();
			parseRow(row);
		}
		logger.debug("解析完成,共解析到{}条记录",rollbackCache.size());
	}
	
	private void parseRow(Element row) {
		String system_id = row.elementText("system_id");
		String original_sql = row.elementText("original_sql");
		String rollback_sql = row.elementText("rollback_sql");
		String indexs = row.elementText("indexs");
		String table = row.elementText("table");
		String sql_type = row.elementText("sql_type");
		String primary_key = row.elementText("primary_key");
		String primary_value_index = row.elementText("primary_value_index");
		String shard_value_index = row.elementText("shard_value_index");
		logger.debug("读取到{}手动回滚配置",original_sql);
		Map<String, Object> _row = new HashMap<>();
		_row.put("system_id", Integer.parseInt(system_id));
		_row.put("original_sql", original_sql);
		_row.put("rollback_sql", rollback_sql);
		_row.put("indexs", indexs.trim());
		_row.put("table", table.trim().toLowerCase());
		_row.put("sql_type", sql_type.trim().toLowerCase());
		_row.put("primary_key", primary_key.trim().toLowerCase());
		_row.put("primary_value_index", primary_value_index.trim());
		_row.put("shard_value_index", shard_value_index);
		rollbackCache.put(original_sql, _row);
	}

}

