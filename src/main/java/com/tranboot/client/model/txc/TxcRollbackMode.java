package com.tranboot.client.model.txc;

public enum TxcRollbackMode {
	SERIAL(1),
	PARALLEL(0)
	
	;
	
	private int mode;
	
	TxcRollbackMode(int mode){
		this.mode = mode;
	}
	
	public int getMode() {
		return mode;
	}
}
