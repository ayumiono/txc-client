package com.tranboot.client.model.txc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 
 * 一个事务对应于一个实体
 * 
 * @author xuelong.chen
 *
 */
public class BranchRollbackMessage implements Serializable{
	
	private static final long serialVersionUID = 694903195528063390L;
	private Long branchId;
	private long xid;
	private List<BranchRollbackInfo> info;
	private String serverIp;
	private String dataSource;
	private int status;
	private Date transactionStartDate;
	private Long transactionOutTimeSecond;
	
	public Long getBranchId() {
		return branchId;
	}
	public void setBranchId(Long branchId) {
		this.branchId = branchId;
	}
	public long getXid() {
		return xid;
	}
	public void setXid(long xid) {
		this.xid = xid;
	}
	public String getServerIp() {
		return serverIp;
	}
	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	
	public Date getTransactionStartDate() {
		return transactionStartDate;
	}
	public void setTransactionStartDate(Date transactionStartDate) {
		this.transactionStartDate = transactionStartDate;
	}
	
	public Long getTransactionOutTimeSecond() {
		return transactionOutTimeSecond;
	}
	public void setTransactionOutTimeSecond(Long transactionOutTimeSecond) {
		this.transactionOutTimeSecond = transactionOutTimeSecond;
	}
	public void addBranchRollbackInfo(String datasource,BranchRollbackInfo info) {
		this.setDataSource(datasource);
		if(this.info == null) {
			this.info = new ArrayList<>();
		}
		this.info.add(info);
	}
	
	public void addBranchRollbackInfo(String datasource,List<BranchRollbackInfo> infos) {
		this.setDataSource(datasource);
		if(this.info == null) {
			this.info = new ArrayList<>();
		}
		this.info.addAll(infos);
	}
	
	public String getDataSource() {
		return dataSource;
	}
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}
	public List<BranchRollbackInfo> getInfo() {
		return info;
	}
	public void setInfo(List<BranchRollbackInfo> info) {
		this.info = info;
	}
	@Override
	public String toString() {
		return "BranchRollbackMessage [branchId=" + branchId + ", xid=" + xid + ", info=" + info + ", serverIp="
				+ serverIp + ", dataSource=" + dataSource + ", status=" + status + ", transactionStartDate="
				+ transactionStartDate + "]";
	}
}
