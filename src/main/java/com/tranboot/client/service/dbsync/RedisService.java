package com.tranboot.client.service.dbsync;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import com.alibaba.fastjson.JSON;
import com.gb.soa.omp.dbsync.model.RdisSyncModel;
import com.gb.soa.omp.dbsync.util.DbsyncUtil;

public class RedisService {
	
	private RedisTemplate<String, RdisSyncModel> dbSyncRedisTemplate;
	
	public RedisService(RedisTemplate<String, RdisSyncModel> dbSyncRedisTemplate) {
		this.dbSyncRedisTemplate = dbSyncRedisTemplate;
	}
	
	/**
	 * json序列化协议
	 * @deprecated 使用Jackson2JsonRedisSerializer
	 * @author xuelong.chen
	 */
	public static final class RdisSyncModelSerializer implements RedisSerializer<RdisSyncModel> {
		
		@Override
		public byte[] serialize(RdisSyncModel t) throws SerializationException {
			return JSON.toJSONBytes(t);
		}

		@Override
		public RdisSyncModel deserialize(byte[] bytes) throws SerializationException {
			if(bytes == null) return null;//jedis中kv set会返回null,如果不加会报错
			return JSON.parseObject(bytes, RdisSyncModel.class);
		}
	}

	public void kvset(String redisKey, RdisSyncModel rdisSyncModel) {
		dbSyncRedisTemplate.opsForValue().set(redisKey, rdisSyncModel);
	}
	
	public RdisSyncModel kvget(String redisKey) {
		return dbSyncRedisTemplate.opsForValue().get(redisKey);
	}
	
	public static String key(String bizKey) {
		return DbsyncUtil.redisKeyStart + System.currentTimeMillis()+"_"+bizKey;
	}

	public RedisTemplate<String, RdisSyncModel> getDbSyncRedisTemplate() {
		return dbSyncRedisTemplate;
	}

	public void setDbSyncRedisTemplate(RedisTemplate<String, RdisSyncModel> dbSyncRedisTemplate) {
		this.dbSyncRedisTemplate = dbSyncRedisTemplate;
	}
}
