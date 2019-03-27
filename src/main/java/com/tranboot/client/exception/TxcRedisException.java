package com.tranboot.client.exception;

public class TxcRedisException extends Exception {
	private static final long serialVersionUID = -8491746756251416352L;

	public TxcRedisException(String message) {
		super(message);
	}
	
	public TxcRedisException(Throwable cause,String message) {
		super(message, cause);
	}
}
