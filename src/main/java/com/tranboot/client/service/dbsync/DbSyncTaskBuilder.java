package com.tranboot.client.service.dbsync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.gb.soa.omp.dbsync.model.MqSyncModel;
import com.gb.soa.omp.dbsync.model.RdisSyncDetailModel;
import com.gb.soa.omp.dbsync.model.RdisSyncModel;
import com.tranboot.client.core.dbsync.DbSyncSqlInvokeContextManager.SQLInvokeContext;
import com.tranboot.client.core.dbsync.DbSyncSqlInvokeContextManager.TransactionPack;
import com.tranboot.client.model.dbsync.SqlTransformResult;
import com.tranboot.client.utils.FlushableLoggerFactory;
import com.tranboot.client.utils.FlushableLoggerFactory.FlushableLogger;

public class DbSyncTaskBuilder {

	private static final FlushableLogger logger =  FlushableLoggerFactory.getLogger(LoggerFactory.getLogger(DbSyncTask.class));
	
	private SqlParserService sqlParserService;
	private DbsyncMqService mqService;
	private RedisService redisService;
	
	public DbSyncTaskBuilder(DbsyncMqService mqService, RedisService redisService) {
		this.sqlParserService = new SqlParserService();
		this.mqService = mqService;
		this.redisService = redisService;
	}
	
	public DbSyncTask build(TransactionPack pack) {
		return new DbSyncTask(pack);
	}

	class DbSyncTask implements Runnable {

		private TransactionPack pack;

		public DbSyncTask(TransactionPack pack) {
			this.pack = pack;
		}
		
		@Override
		public String toString() {
			return "DbsyncTask【"+pack.getTransactionNo()+"】";
		}

		@Override
		public void run() {
			List<String> sqls = new ArrayList<String>();
			List<RdisSyncDetailModel> redisDetails = new ArrayList<>();
			logger.debug(pack.getTransactionNo()+"-->开始解析sql任务");
			String titleBizKey = null, titleSourceDb = null, titleSourceTable = null, titleTargetDB = null, titleTargetTable = null;
			long timeStamp = System.currentTimeMillis();
			logger.debug(String.format("dbsync sqllog start:{'time':%d,'transactionId':%s}", timeStamp,pack.getTransactionNo()));
			try {
				for (SQLInvokeContext context : pack.getSqlinvokecontexts()) {
					String sql = context.getSql();
					logger.debug(pack.getTransactionNo()+"-->原sql:"+sql);
					Object[] args = context.getArgs();
					SqlTransformResult result = sqlParserService.parseSql(sql);
					if (result == null || result == SqlTransformResult.a) {
						continue;
					}else if(result == SqlTransformResult.b){
						logger.error(String.format("sql语句:%s没有主键，无法进行数据双写操作!!!", sql));
						continue;
					}
					try {
						String sql2Send = result.sql(args);
						sqls.add(sql2Send);
						logger.debug(pack.getTransactionNo()+"-->同步sql:"+sql2Send);
					} catch (Exception e) {
						logger.error(pack.getTransactionNo()+"-->双写任务-分析业务sql失败"+sql, e);
					}
					
					logger.debug(pack.getTransactionNo()+"-->开始构造model-----"+pack.getTransactionNo());
					
					RdisSyncDetailModel redisDetailModel = new RdisSyncDetailModel();
					logger.debug(pack.getTransactionNo()+"-->开始设置主键字段");
					redisDetailModel.setColumnKeyName(result.getPrimaryKey());
					logger.debug(pack.getTransactionNo()+"-->开始设置主键值");
					List<String> vv = Arrays.asList(result.primaryKeyValue(args));
					logger.debug(pack.getTransactionNo()+"-->主键值数量:"+vv.size());
					redisDetailModel.setColumnKeyValue(vv);
					logger.debug(pack.getTransactionNo()+"-->开始设置主键值标题");
					if (titleBizKey == null) {
						titleBizKey = redisDetailModel.getColumnKeyValue().get(0);
					}
					logger.debug(pack.getTransactionNo()+"-->开始设置源表标题");
					redisDetailModel.setSourceTable(result.getSourceTable());
					if (titleSourceTable == null) {
						titleSourceTable = result.getSourceTable();
					}
					logger.debug(pack.getTransactionNo()+"-->开始设置源表分库字段");
					redisDetailModel.setSourceTablePartitionName(result.getSourcePartitionKey());
					logger.debug(pack.getTransactionNo()+"-->开始设置源表分库字段值");
					redisDetailModel.setSourceTablePartitionValue(result.sourcePartitionValue(args));
					logger.debug(pack.getTransactionNo()+"-->开始设置目标表分库字段");
					redisDetailModel.setTargetTablePartitionName(result.getTargetPartitionKey());
					logger.debug(pack.getTransactionNo()+"-->开始设置目标表分库字段值");
					redisDetailModel.setTargetTablePartitionValue(result.targetPartitionValue(args));
					logger.debug(pack.getTransactionNo()+"-->开始设置源表数据库类型标题");
					if (titleSourceDb == null) {
						titleSourceDb = result.getSourceDb();
					}
					logger.debug(pack.getTransactionNo()+"-->开始设置目标表数据库类型标题");
					if (titleTargetDB == null) {
						titleTargetDB = result.getTargetDb();
					}
					logger.debug(pack.getTransactionNo()+"-->开始设置目标表标题");
					if(titleTargetTable == null) {
						titleTargetTable = result.getTargetTable();
					}
					redisDetails.add(redisDetailModel);
					logger.debug(pack.getTransactionNo()+"-->构造model完成");
				}
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(pack.getTransactionNo()+"-->任务处理-sql解析失败", e);
				return;
			}
			
			if(redisDetails.size() == 0) {
				logger.debug(String.format(pack.getTransactionNo()+"-->没有需要双写的sql"));
				return;
			}
			
			logger.debug(String.format("dbsync sqllog end:{'time':%d,'transactionId':%s}", timeStamp,pack.getTransactionNo()));
			MqSyncModel mqSyncModel = new MqSyncModel();
			mqSyncModel.setSql(sqls);
			mqSyncModel.setTargetDb(titleTargetDB);
			mqSyncModel.setTargetTable(titleTargetTable);
			mqSyncModel.setColumnKeyValue(titleBizKey);
			String redisKey = RedisService.key(titleBizKey);
			mqSyncModel.setRedisKey(redisKey);
			RdisSyncModel redisModel = new RdisSyncModel();
			redisModel.setRdisSyncDetailModel(redisDetails);
			redisModel.setSourceDb(titleSourceDb);
			redisModel.setTargetDb(titleTargetDB);
//			String messageKey = DbsyncUtil.buildMqMessageKey(titleBizKey, titleSourceDb, titleSourceTable,
//					titleTargetDB);
			String messageKey = DbsyncMqService.messageKey(titleBizKey, titleSourceDb, titleSourceTable,
					titleTargetDB);
			mqSyncModel.setMessageKey(messageKey);
			redisModel.setMessageKey(messageKey);
			logger.debug(String.format(pack.getTransactionNo()+"-->存入redis kv-> key:%s", redisKey));
			try {
				redisService.kvset(redisKey, redisModel);
				logger.debug(pack.getTransactionNo()+"-->存入redis kv成功");
			} catch (Exception e) {
				logger.error(pack.getTransactionNo()+"-->双写任务-写redis失败", e);
				return;
			}
			try {
				logger.debug(String.format(pack.getTransactionNo()+"-->发送mq消息-> key:%s", redisModel.getMessageKey()));
				mqService.send(mqSyncModel, redisModel);
				logger.debug(pack.getTransactionNo()+"-->发送mq消息成功");
			} catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
				logger.error(pack.getTransactionNo()+"-->双写任务-发mq失败", e);
				e.printStackTrace();
				return;
			} 
			logger.debug(pack.getTransactionNo()+"-->解析sql任务结束");
			logger.flush();
		}
	}
	
}
