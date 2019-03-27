package com.tranboot.client.sqlast;

public interface SqlInsertStatementBuilder {
	public SqlInsertStatementBuilder insert(String table);
	public SqlInsertStatementBuilder column(String... columns);
}
