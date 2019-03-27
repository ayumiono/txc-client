package com.tranboot.client.model;

public enum SQLType {
	INSERT("insert",0),
	UPDATE("udpate",1),
	DELETE("delete",2),
	SELECT("select",3);
	
	private String value;
	private int code;
	
	private SQLType(String type,int code) {
		this.value = type;
		this.code = code;
	}
	
	public String getType() {
		return this.value;
	}
	
	public int getCode() {
		return this.code;
	}
	
	public static SQLType type(String sqlType) {
		switch (sqlType) {
		case "update":
			return SQLType.UPDATE;
		case "insert":
			return SQLType.INSERT;
		case "delete":
			return SQLType.DELETE;
		case "select":
			return SQLType.SELECT;
		default:
			return null;
		}
	}
}
