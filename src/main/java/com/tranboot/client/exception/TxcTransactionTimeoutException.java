package com.tranboot.client.exception;

/**
 * 
 * thread local中管理的资源状态错误
 * 
 * @author xuelong.chen
 *
 */
public class TxcTransactionTimeoutException extends RuntimeException {

	private static final long serialVersionUID = 9106036746676443390L;

	public TxcTransactionTimeoutException(String message) {
		super(message);
	}
	
	public TxcTransactionTimeoutException(Throwable cause,String message) {
		super(message, cause);
	}
}
