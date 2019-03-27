package com.tranboot.client.service.txc.impl;

import static com.tranboot.client.utils.MetricsReporter.queryManualRollbackSql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.codahale.metrics.Timer;
import com.tranboot.client.exception.TxcTransactionException;
import com.tranboot.client.service.txc.TxcManualRollbackSqlService;
import com.tranboot.client.spring.ContextUtils;

@Deprecated
public class TxcManualRollbackSQLServiceMysqlImpl extends TxcManualRollbackSqlService{
	
	int systemId;
	
	public TxcManualRollbackSQLServiceMysqlImpl(int systemId) {
		this.systemId = systemId;
		init();
	}

	@Override
	public void init() {
		queryManualRollbackSql();
	}

	private void queryManualRollbackSql() {
		logger.debug("开始从数据库中读取并加载数据库缓存 {}",systemId);
		Connection con = null;
		Timer.Context context = queryManualRollbackSql.time();
		try {
			String sql = "select * from txc_manual_rollback_sql where systemId=?";
			DataSource platformDs = ContextUtils.getBean("platformDataSource", DataSource.class);
			con = platformDs.getConnection();
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setInt(1, systemId);
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("original_sql", rs.getString("original_sql"));
				row.put("rollback_sql", rs.getString("rollback_sql"));
				row.put("system_id", rs.getInt("system_id"));
				row.put("indexs", rs.getString("indexs"));
				row.put("table", rs.getString("table"));
				row.put("sql_type", rs.getString("sql_type").trim().toLowerCase());
				row.put("primary_key", rs.getString("primary_key"));
				row.put("primary_value_index", rs.getString("primary_value_index"));
				row.put("shard_value_index", rs.getString("shard_value_index"));
				dealRow(row);
			}
			logger.debug("加载数据库缓存结束,共读取到{}条数据",rollbackCache.size());
		} catch (Exception e) {
			throw new TxcTransactionException(e, "txc_manual_rollback_sql查询失败");
		} finally {
			try {
				if(con != null) {
					con.close();
				}
				context.stop();
			} catch (SQLException e) {
				logger.error("手动释放连接失败", e);
				context.stop();
				throw new TxcTransactionException(e, "释放连接失败");
			}
		}
	}
	
	private void dealRow(Map<String, Object> row) {
		rollbackCache.put(row.get("original_sql").toString(), row);
	}

}

