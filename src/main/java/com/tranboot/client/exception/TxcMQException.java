package com.tranboot.client.exception;

public class TxcMQException extends Exception{

	private static final long serialVersionUID = 6982830247892285837L;

	public TxcMQException(String message) {
		super(message);
	}
	
	public TxcMQException(Throwable cause,String message) {
		super(message, cause);
	}
}
