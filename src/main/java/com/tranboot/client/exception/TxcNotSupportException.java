package com.tranboot.client.exception;

/**
 * 
 * sql解析时，遇到无法支持的语法
 * 
 * @author xuelong.chen
 *
 */
public class TxcNotSupportException extends RuntimeException {

	private static final long serialVersionUID = 3763827537077434132L;
	
	public TxcNotSupportException(String message) {
		super(message);
	}
	
	public TxcNotSupportException(Throwable cause,String message) {
		super(message, cause);
	}

}
