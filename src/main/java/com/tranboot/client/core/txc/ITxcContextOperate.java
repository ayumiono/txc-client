package com.tranboot.client.core.txc;

public interface ITxcContextOperate {
	
	public Object getUserData(String paramString);

	public Object putUserData(String paramString1, Object paramString2);

	public Object removeUserData(String paramString);

	public Object getRpcContext();
	
	public Object removeUserData();
	
	public Object removeRpcContext();
}
