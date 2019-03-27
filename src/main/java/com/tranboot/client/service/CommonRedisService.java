package com.tranboot.client.service;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import com.alibaba.fastjson.JSON;

public class CommonRedisService<K,V,HK,HV> {
	
	private RedisTemplate<K, V> redisTemplate;
	
	public CommonRedisService(){
		redisTemplate = new RedisTemplate<>();
		redisTemplate.setHashKeySerializer(new CommonJsonSerializer<HK>(null));
		redisTemplate.setHashValueSerializer(new CommonJsonSerializer<HV>(null));
		
	}
	
	public static final class CommonJsonSerializer<T> implements RedisSerializer<T> {
		
		private Class<T> clazz;
		
		public CommonJsonSerializer(Class<T> clazz) {
			this.clazz = clazz;
		}

		@Override
		public byte[] serialize(T t) throws SerializationException {
			return JSON.toJSONBytes(t);
		}

		@Override
		public T deserialize(byte[] bytes) throws SerializationException {
			if(bytes == null) return null;//jedis中kv set会返回null,如果不加会报错
			return JSON.parseObject(bytes, clazz);
		}
		
	}
}
