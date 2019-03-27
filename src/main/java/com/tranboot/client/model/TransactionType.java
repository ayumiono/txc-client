package com.tranboot.client.model;

public enum TransactionType {
	EXIST(1),
	NOTEXIST(0);
	
	int code;
	
	private TransactionType(int code) {
		this.code = code;
	}
	
	public int code() {
		return code;
	}
	
	public static boolean inTransaction(int code) {
		if(code == 0) return false;
		if(code == 1) return true;
		throw new IllegalStateException("不支持的事务类型");
	}
}
