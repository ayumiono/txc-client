package com.tranboot.client.model.dbsync;

public class SqlParser {

	private String id;
	
	private SqlRouter router;
	private SqlMapper mapper;

	public SqlParser(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}


	public SqlRouter getRouter() {
		return router;
	}

	public void setRouter(SqlRouter router) {
		this.router = router;
	}

	public SqlMapper getMapper() {
		return mapper;
	}

	public void setMapper(SqlMapper mapper) {
		this.mapper = mapper;
	}
}
