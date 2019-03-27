package com.tranboot.client.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

/**
 * mysql表信息缓存数据，主要用来确定主键字段
 * 
 * 这里的主键字段既包括mysql中的主键，也包括mysql中的uniq索引。目前不支持联合唯一索引，因为获取不到这类信息
 * 
 * @author xuelong.chen
 *
 */
public class TableSchema {
	
	/**
	 * 表名
	 */
	private String tableName;
	
	private String dbName;
	
	private List<String> primaryKeyStrs = new ArrayList<>();
	private Collection<List<String>> uniqKeyStrs;
	
	/**
	 * 分库字段名
	 */
	private String shardField;
	
	public String getShardField() {
		return shardField;
	}

	public void setShardField(String shardField) {
		this.shardField = shardField;
	}
	
	/**
	 * 所有字段集合
	 */
	private List<String> columns;
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	
	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	
	public void setUniqKeyStrs(Collection<List<String>> uniqkeys) {
		uniqKeyStrs = uniqkeys;
	}
	
	public Collection<List<String>> getUniqKeyStrs() {
		return uniqKeyStrs;
	}

	public void primaryKey(String column) {
		primaryKeyStrs.add(column);
	}
	
	public void setPrimaryKeyStrs(List<String> primaryKeyStrs) {
		this.primaryKeyStrs = primaryKeyStrs;
	}
	
	public List<String> getPrimaryKeyStrs() {
		return primaryKeyStrs;
	}

	/**
	 * 主键为空，则取第一组唯一索引
	 * @return
	 */
	public List<String> getPK() {
		if(primaryKeyStrs.size() <= 0) {
			return uniqKeyStrs.iterator().next();
		}else {
			return primaryKeyStrs;
		}
	}
	
	/**
	 * 根据给定的列，匹配主键或一组唯一索引，用于insert语句
	 * @param columns
	 * @return
	 */
	public List<String> matchPK(Set<String> columns){
		boolean chosePrimary = true;
		for(String col : primaryKeyStrs) {
			if(!columns.contains(col)) {
				chosePrimary = false;
				break;
			}
		}
		
		if(chosePrimary) return primaryKeyStrs;
		
		nextset : for(List<String> uniqkeys : uniqKeyStrs) {
			for(String col : uniqkeys) {
				if(!columns.contains(col)) {
					continue nextset;
				}
			}
			return uniqkeys;
		}
		
		return null;
	}
	
	@Override
	public String toString() {
		StringBuilder sbuilder = new StringBuilder();
		sbuilder.append("【数据库名】:").append(this.dbName).append(System.lineSeparator());
		sbuilder.append("【表名】:").append(this.tableName).append(System.lineSeparator());
		sbuilder.append("【主键字段】:");
		if(primaryKeyStrs == null || primaryKeyStrs.size() <= 0) {
			sbuilder.append("无");
		}else {
			sbuilder.append(System.lineSeparator());
			for(String primaryKeyStr : primaryKeyStrs) {
				sbuilder.append(primaryKeyStr).append("\t");
			}
		}
		
		sbuilder.append(System.lineSeparator());
		sbuilder.append("【唯一索引】:");
		if(uniqKeyStrs == null || uniqKeyStrs.size() <= 0) {
			sbuilder.append("无");
		}else {
			sbuilder.append(System.lineSeparator());
			for(List<String> index : uniqKeyStrs) {
				sbuilder.append(StringUtils.join(index, ","));
				sbuilder.append(System.lineSeparator());
			}
		}
		sbuilder.append(System.lineSeparator());
		sbuilder.append("【表字段】:").append(System.lineSeparator());
		sbuilder.append(StringUtils.join(columns, ","));
		return sbuilder.toString();
	}
	
//	@Override
//	public String toString() {
//		StringBuilder sbuilder = new StringBuilder();
//		sbuilder.append("【数据库名】:").append(this.dbName).append(System.lineSeparator());
//		sbuilder.append("【表名】:").append(this.tableName).append(System.lineSeparator());
//		sbuilder.append("【主键字段】:");
//		if(primaryKeyStrs == null) {
//			sbuilder.append("无");
//		}
//		for(String primaryKeyStr : primaryKeyStrs) {
//			sbuilder.append(primaryKeyStr).append("\t");
//		}
//		sbuilder.append(System.lineSeparator());
//		sbuilder.append("【表字段】:").append(System.lineSeparator());
//		try {
//			for (Field field : fields) {
//				sbuilder.append(field.getName()).append("\t");
//				switch (field.getSQLType()) {
//				case java.sql.Types.BIGINT:
//					sbuilder.append("bigint");
//					break;
//				case java.sql.Types.BOOLEAN:
//					sbuilder.append("boolean");
//					break;
//				case java.sql.Types.CHAR:
//					sbuilder.append("char");
//					break;
//				case java.sql.Types.DATE:
//					sbuilder.append("date");
//					break;
//				case java.sql.Types.DECIMAL:
//					sbuilder.append("decimal");
//					break;
//				case java.sql.Types.DOUBLE:
//					sbuilder.append("double");
//					break;
//				case java.sql.Types.FLOAT:
//					sbuilder.append("float");
//					break;
//				case java.sql.Types.INTEGER:
//					sbuilder.append("integer");
//					break;
//				case java.sql.Types.VARCHAR:
//					sbuilder.append("varchar");
//					break;
//				case java.sql.Types.TINYINT:
//					sbuilder.append("tinyint");
//					break;
//				case java.sql.Types.TIMESTAMP:
//					sbuilder.append("timestamp");
//					break;
//				case java.sql.Types.TIME:
//					sbuilder.append("time");
//					break;
//				case java.sql.Types.NVARCHAR:
//					sbuilder.append("nvarchar");
//					break;
//				case java.sql.Types.SMALLINT:
//					sbuilder.append("smallint");
//					break;
//				case java.sql.Types.LONGVARCHAR:
//					sbuilder.append("text");
//					break;
//				default:
//					sbuilder.append("unknown").append("(").append(field.getSQLType()).append(")");
//					break;
//				}
//				sbuilder.append("\t")
//				.append(field.getLength()).append("\t")
//				.append(field.getEncoding()).append("\t");
//				if(field.isPrimaryKey()) {
//					sbuilder.append("Y");
//				}
//				sbuilder.append(System.lineSeparator());
//			}
//		} catch (Exception e) {
//			sbuilder.append("表字段获取失败:"+e.getMessage());
//		}
//		return sbuilder.toString();
//	}
}