package com.tranboot.client.exception;

public class TxcTransactionException extends RuntimeException {

	public TxcTransactionException(String message) {
		super(message);
	}
	
	public TxcTransactionException(Throwable cause,String message) {
		super(message, cause);
	}

	private static final long serialVersionUID = 6126448585923572058L;
	
}
