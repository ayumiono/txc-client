package com.tranboot.client.exception;

public class TxcDBException extends Exception{

	private static final long serialVersionUID = 6982830247892285837L;

	public TxcDBException(String message) {
		super(message);
	}
	
	public TxcDBException(Throwable cause,String message) {
		super(message, cause);
	}
}
