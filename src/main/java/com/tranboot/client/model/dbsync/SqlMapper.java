package com.tranboot.client.model.dbsync;

import java.util.Map;
import java.util.Set;

public class SqlMapper {

	private String targetDb;
	private String sourceDb;
	private String targetDbType;
	private String sourceDbType;
	private String targetTable;
	private String sourceTable;
	private String sourceKeyField;
	private String targetKeyField;
	
	private Map<String, String> fieldMapper;
	private Set<String> excludeFields;
	
	
	public boolean needExclude(String field) {
		if(excludeFields.contains(field.toLowerCase())) return true;
		return false;
	}
	
	public String mapField(String field) {
		if(fieldMapper.get(field.toLowerCase()) == null) {
			return field;
		}
		return fieldMapper.get(field.toLowerCase());
	}
	
	public String getTargetDb() {
		return targetDb;
	}

	public void setTargetDb(String targetDb) {
		this.targetDb = targetDb;
	}

	public String getSourceDb() {
		return sourceDb;
	}

	public void setSourceDb(String sourceDb) {
		this.sourceDb = sourceDb;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}

	public String getSourceKeyField() {
		return sourceKeyField;
	}

	public void setSourceKeyField(String sourceKeyField) {
		this.sourceKeyField = sourceKeyField;
	}

	public String getTargetKeyField() {
		return targetKeyField;
	}

	public void setTargetKeyField(String targetKeyField) {
		this.targetKeyField = targetKeyField;
	}
	
	public Map<String, String> getFieldMapper() {
		return fieldMapper;
	}

	public void setFieldMapper(Map<String, String> fieldMapper) {
		this.fieldMapper = fieldMapper;
	}

	public Set<String> getExcludeFields() {
		return excludeFields;
	}

	public void setExcludeFields(Set<String> excludeFields) {
		this.excludeFields = excludeFields;
	}

	public String getTargetDbType() {
		return targetDbType;
	}

	public void setTargetDbType(String targetDbType) {
		this.targetDbType = targetDbType;
	}

	public String getSourceDbType() {
		return sourceDbType;
	}

	public void setSourceDbType(String sourceDbType) {
		this.sourceDbType = sourceDbType;
	}

	@Override
	public String toString() {
		return "SqlMapper [" + targetDb + "->" + sourceDb + ", " + targetTable + "->" + sourceTable + sourceKeyField
				+ "->" + targetKeyField + "]";
	}
}
