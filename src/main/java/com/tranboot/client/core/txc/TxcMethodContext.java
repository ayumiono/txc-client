package com.tranboot.client.core.txc;

import java.lang.reflect.Method;

public class TxcMethodContext {
	private TxcTransaction txcTransaction;
	private Method localMethod;
	
	public TxcMethodContext(TxcTransaction annotation,Method method) {
		this.txcTransaction = annotation;
		this.localMethod = method;
	}

	public TxcTransaction getTxcTransaction() {
		return txcTransaction;
	}

	public void setTxcTransaction(TxcTransaction txcTransaction) {
		this.txcTransaction = txcTransaction;
	}

	public Method getLocalMethod() {
		return localMethod;
	}

	public void setLocalMethod(Method localMethod) {
		this.localMethod = localMethod;
	}
}
