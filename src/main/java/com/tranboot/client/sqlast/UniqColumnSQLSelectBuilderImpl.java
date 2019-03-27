package com.tranboot.client.sqlast;

import java.util.HashSet;
import java.util.Set;

import com.tranboot.client.druid.sql.builder.impl.SQLSelectBuilderImpl;

/**
 * 增加selectColumns排重
 * @author xuelong.chen
 *
 */
public class UniqColumnSQLSelectBuilderImpl extends SQLSelectBuilderImpl {
	
	Set<String> selectColumns;
	
	public UniqColumnSQLSelectBuilderImpl(String dbType) {
		super(dbType);
	}
	
	@Override
	public SQLSelectBuilderImpl select(String... columns) {
		if(selectColumns == null) {
			selectColumns = new HashSet<>();
		}
		for(String column : columns) {
			if(column == null) continue;
			if(!selectColumns.contains(column)) {
				super.select(column);
			}
		}
		return this;
    }
}

