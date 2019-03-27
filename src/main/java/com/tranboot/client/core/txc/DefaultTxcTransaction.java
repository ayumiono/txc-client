package com.tranboot.client.core.txc;

import java.lang.annotation.Annotation;

import com.tranboot.client.model.txc.TxcRollbackMode;

public final class DefaultTxcTransaction implements TxcTransaction{

	@Override
	public Class<? extends Annotation> annotationType() {
		return null;
	}

	@Override
	public TxcRollbackMode rollbackMode() {
		return TxcRollbackMode.PARALLEL;
	}

	@Override
	public int timeout() {
		return 0;
	}
}
