package com.tranboot.client.model;

public class MysqlIndex {
	private String table;
	private int nonUnique;
	private String keyName;
	private int seqInIndex;
	private String columnName;
	private String indexType;
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public int getNonUnique() {
		return nonUnique;
	}
	public void setNonUnique(int nonUnique) {
		this.nonUnique = nonUnique;
	}
	public String getKeyName() {
		return keyName;
	}
	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}
	public int getSeqInIndex() {
		return seqInIndex;
	}
	public void setSeqInIndex(int seqInIndex) {
		this.seqInIndex = seqInIndex;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getIndexType() {
		return indexType;
	}
	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}
}