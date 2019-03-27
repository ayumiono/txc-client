package com.tranboot.client.exception;

public class TxcQueryException extends RuntimeException {

	public TxcQueryException(String message) {
		super(message);
	}
	
	public TxcQueryException(Throwable cause,String message) {
		super(message, cause);
	}
	
	private static final long serialVersionUID = -186673934205383166L;

}
