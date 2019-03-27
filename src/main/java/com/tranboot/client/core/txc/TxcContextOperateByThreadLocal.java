package com.tranboot.client.core.txc;

import java.util.HashMap;
import java.util.Map;

public class TxcContextOperateByThreadLocal implements ITxcContextOperate {

	private static final ThreadLocal<Map<Object, Object>> _threadLocalContext = new ThreadLocal<>();
	
	@Override
	public Object getUserData(String paramString) {
		return get().get(paramString);
	}

	@Override
	public Object putUserData(String key, Object value) {
		return get().put(key, value);
	}

	@Override
	public Object removeUserData(String key) {
		return get().remove(key);
	}

	@Override
	public Object getRpcContext() {
		return null;
	}
	
	private Map<Object, Object> get(){
		if(_threadLocalContext.get() == null) {
			_threadLocalContext.set(new HashMap<>());
		}
		return _threadLocalContext.get();
	}

	@Override
	public Object removeUserData() {
		Object t = _threadLocalContext.get();
		if(t != null) {
			_threadLocalContext.set(null);
			_threadLocalContext.remove();
		}
		return t;
	}

	@Override
	public Object removeRpcContext() {
		return null;
	}
}
