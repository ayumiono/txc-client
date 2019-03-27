package com.tranboot.client.model.txc;

import java.io.Serializable;
import java.util.List;

import com.gb.soa.omp.transaction.model.SqlParamModel;

/**
 * 一条sql对应于一个实体
 * @author xuelong.chen
 *
 */
public class BranchRollbackInfo implements Serializable{
	
	private static final long serialVersionUID = 5006330756385094572L;
	
	private List<SqlParamModel> rollbackSql;
	
	public List<SqlParamModel> getRollbackSql() {
		return rollbackSql;
	}
	public void setRollbackSql(List<SqlParamModel> rollbackSql) {
		this.rollbackSql = rollbackSql;
	}
}
