package com.tranboot.client.model.dbsync;

public interface RoutePolicy {
	public String route(String key,Object value,int partionNum);
}
