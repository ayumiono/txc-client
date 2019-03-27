package com.tranboot.client.service.txc;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.gb.soa.omp.transaction.model.RedisTransactionModel;
import com.gb.soa.omp.transaction.model.SqlParamModel;
import com.gb.soa.omp.transaction.util.TransactionApiUtil;
import com.tranboot.client.exception.TxcRedisException;
import com.tranboot.client.model.txc.BranchRollbackInfo;
import com.tranboot.client.model.txc.BranchRollbackMessage;
import static com.tranboot.client.utils.MetricsReporter.*;

public class TxcRedisService {
	
	protected Logger logger = LoggerFactory.getLogger(TxcRedisService.class);
	
	public static final int INIT_STATUS = 0;
	public static final int COMMITTABLE_STATUS = 1;
	public static final int ROLLBACKABLE_STATUS = 2;
	
	private RedisTemplate<String, RedisTransactionModel> txcRedisTemplate;
	
	private StringRedisTemplate stringRedisTemplate;
	
	public TxcRedisService(RedisTemplate<String, RedisTransactionModel> txcRedisTemplate, StringRedisTemplate stringRedisTemplate) {
		this.txcRedisTemplate = txcRedisTemplate;
		this.stringRedisTemplate = stringRedisTemplate;
	}
	
	public long getCurrentTimeMillisFromRedis() throws TxcRedisException {
		Timer.Context context= time.time();
		try {
			return stringRedisTemplate.execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
					return redisConnection.time();
				}
			});
		} catch (Exception e) {
			throw new TxcRedisException(e, "获取redis 系统时间失败");
		} finally {
			context.stop();
		}
	}
	
	public void hput(RedisTransactionModel model) throws TxcRedisException {
		Timer.Context context = hput.time();
		try {
			logger.debug("redis hput:{}",model.toString());
			txcRedisTemplate.opsForHash().put(TransactionApiUtil.redisKeyStart+model.getTransactionId(), String.valueOf(model.getTransactionSqlId()), model);
			logger.debug("redis hput finished",model.toString());
		} catch (Exception e) {
			throw new TxcRedisException(e,"txc reids存入失败");
		} finally {
			context.stop();
		}
	}
	
	public void hput(List<RedisTransactionModel> models) throws TxcRedisException {
		for(RedisTransactionModel model : models) {
			hput(model);
		}
	}
	
	public static RedisTransactionModel buildTransactionModel(BranchRollbackMessage message) {
		RedisTransactionModel model = new RedisTransactionModel();
		model.setSourceDb(message.getDataSource());
		model.setTransactionId(message.getXid());
		model.setTransactionSqlId(message.getBranchId());//branchid 分支事务id
		model.setTransactionStartDtme(message.getTransactionStartDate());
		model.setStatus(message.getStatus());
		List<SqlParamModel> sqls = new ArrayList<>();
		if(message.getInfo() != null) {
			for(BranchRollbackInfo info : message.getInfo()) {
				sqls.addAll(info.getRollbackSql());
			}
		}
		model.setSqlParamModel(sqls);
		model.setTransactionOutTimeSecond(message.getTransactionOutTimeSecond());//FIXME
		return model;
	}
}

