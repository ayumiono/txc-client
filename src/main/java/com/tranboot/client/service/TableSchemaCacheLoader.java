package com.tranboot.client.service;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.tranboot.client.exception.TxcNotSupportException;
import com.tranboot.client.model.MysqlIndex;
import com.tranboot.client.model.TableSchema;
import com.tranboot.client.service.txc.TxcShardSettingReader;
import com.tranboot.client.spring.ContextUtils;
import com.tranboot.client.utils.LRUCache;

/**
 * 数据库表schema信息加载器
 * 使用select * from table limit 0,1查询 
 * @author xuelong.chen
 *
 */
public class TableSchemaCacheLoader implements Callable<TableSchema>{
	
	private JdbcTemplate jdbcTemplate;
	
	private String tableName;
	
	private String datasource;
	
	private static final String sql = "select * from %s limit 0,1";
	private static final String _sql = "select table_schema,column_name from information_schema.columns where table_name='%s'";
	private static final String showindex = "show index from %s";
	
	public TableSchemaCacheLoader(JdbcTemplate jdbcTemplate, String tableName, String datasource) {
		this.jdbcTemplate = jdbcTemplate;
		this.tableName = tableName;
		this.datasource = datasource;
	}
	
	private TableSchema loadSchema() {
		TableSchema schema = new TableSchema();
		schema.setTableName(tableName);
		schema.setDbName(datasource);
		List<MysqlIndex> indexs = jdbcTemplate.query(String.format(showindex, tableName),new ResultSetExtractor<List<MysqlIndex>>() {
			@Override
			public List<MysqlIndex> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<MysqlIndex> indexs = new ArrayList<>();
				while(rs.next()) {
					MysqlIndex index = new MysqlIndex();
					index.setTable(rs.getString("Table"));
					index.setNonUnique(rs.getInt("Non_unique"));
					index.setKeyName(rs.getString("Key_name"));
					index.setColumnName(rs.getString("Column_name"));
					index.setIndexType(rs.getString("Index_type"));
					index.setSeqInIndex(rs.getInt("Seq_in_index"));
					indexs.add(index);
				}
				return indexs;
			}});
		
		Map<String, List<String>> tmp = new HashMap<>();
		//提取出Primary key
		for(MysqlIndex index : indexs) {
			if(index.getKeyName().equals("PRIMARY")) {
				schema.primaryKey(index.getColumnName().toLowerCase());
			}else if(index.getNonUnique() == 0){//唯一索引
				String keyname = index.getKeyName();
				if(tmp.containsKey(keyname)) {
					tmp.get(keyname).add(index.getColumnName().toLowerCase());
				}else {
					tmp.put(keyname, new ArrayList<>());
				}
			}
		}
		schema.setUniqKeyStrs(tmp.values());
		if((schema.getPrimaryKeyStrs() == null || schema.getPrimaryKeyStrs().size() <= 0) && 
				(schema.getUniqKeyStrs() == null || schema.getUniqKeyStrs().size() <= 0)) {
			throw new TxcNotSupportException(String.format("表%s没有解析到主键或唯一索引，不能进行分布式事务操作", tableName));
		}
		List<Map<String, String>> allfileds = new ArrayList<>();
		allfileds = jdbcTemplate.query(String.format(_sql, tableName),
				new ResultSetExtractor<List<Map<String, String>>>() {
					@Override
					public List<Map<String, String>> extractData(ResultSet rs) throws SQLException, DataAccessException {
						List<Map<String, String>> result = new ArrayList<>();
						while(rs.next()) {
							Map<String, String> field = new HashMap<>();
							field.put("table_schema", rs.getString("table_schema").toLowerCase());
							field.put("column_name", rs.getString("column_name").toLowerCase());
							result.add(field);
						}
						return result;
					}
				}
		);
		
		if(allfileds == null || allfileds.size() <= 0) {
			jdbcTemplate.query(String.format(sql, tableName),
					new ResultSetExtractor<Map<String, Object>>() {
						@Override
						public Map<String, Object> extractData(ResultSet rs) throws SQLException, DataAccessException {
							ResultSetMetaData rsm = rs.getMetaData();
							try {
								if(Class.forName("com.alibaba.druid.proxy.jdbc.ResultSetMetaDataProxyImpl").isInstance(rsm)) {//druid 中对ResultSetMetaData作了包装
									Method getResultSetMetaDataRaw = Class.forName("com.alibaba.druid.proxy.jdbc.ResultSetMetaDataProxyImpl").getMethod("getResultSetMetaDataRaw");
									rsm = (ResultSetMetaData) getResultSetMetaDataRaw.invoke(rsm);//com.mysql.jdbc.ResultSetMetaData
								}
							} catch (Exception e) {
								throw new TxcNotSupportException(e,String.format("获取表%s table_schema 时出现异常",tableName));
							}
							
							try {
								if(!LRUCache.tableSchemaCacheContain(tableName)) {
									schema.setTableName(tableName);
									Field f_fields = rsm.getClass().getDeclaredField("fields");
									f_fields.setAccessible(true);
									com.mysql.jdbc.Field[] fields = (com.mysql.jdbc.Field[]) f_fields.get(rsm);
									List<String> fs = Arrays.asList(fields).stream().map(new Function<com.mysql.jdbc.Field, String>() {

										@Override
										public String apply(com.mysql.jdbc.Field t) {
											try {
												return t.getName().toLowerCase();
											} catch (Exception e) {
												throw new TxcNotSupportException(e, String.format("获取表%s 列失败", tableName));
											}
										}
									}).collect(Collectors.toList());
									schema.setColumns(fs);
								}
							} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
								throw new TxcNotSupportException(e,String.format("获取表%s table_schema 时出现异常",tableName));
							} 
							return null;
						}
					});
			if(schema.getColumns() == null) {
				throw new TxcNotSupportException(String.format("表%s没有解析到列，不能进行分布式事务操作", tableName));
			}
		}else {
			Map<String,List<Map<String,String>>> grouprs = allfileds.stream().collect(Collectors.groupingBy(new Function<Map<String,String>, String>() {

				@Override
				public String apply(Map<String, String> t) {
					return t.get("table_schema");
				}
			}));
			List<String> fileds = grouprs.entrySet().iterator().next().getValue().stream().map(new Function<Map<String,String>, String>() {

				@Override
				public String apply(Map<String, String> t) {
					return t.get("column_name");
				}
			}).collect(Collectors.toList());
			schema.setColumns(fileds);
		}
		
		String shardField = ContextUtils.getBean(TxcShardSettingReader.class).shardFiled(datasource, tableName);
		schema.setShardField(shardField);
		return schema;
	}

	@Override
	public TableSchema call() throws Exception {
		return loadSchema();
	}
}

