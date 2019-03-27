package com.tranboot.client.core.txc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 
 * 扩展RedisLock，使其可重入
 * @author xuelong.chen
 *
 */
public final class ReentrantRedLock {
	
	private static final ThreadLocal<List<ReentrantRedLock>> holdedLock = new ThreadLocal<>();
	
	private RedisLock innerLock;
	
	public String getRedLockValue() {
		return innerLock.value;
	}
	
	private ReentrantRedLock(RedisLock innerLock) {
		this.innerLock = innerLock;
	}
	
	private ReentrantRedLock(StringRedisTemplate stringRedisTemplate, String lockKey, int expireSecond) {
		this.innerLock = new RedisLock(stringRedisTemplate, lockKey, expireSecond);
	}
	
	protected static ReentrantRedLock redisLock(StringRedisTemplate stringRedisTemplate, String lockKey, int expireSecond) {
		return new ReentrantRedLock(stringRedisTemplate, lockKey, expireSecond);
	}
	
	public boolean tryLock(long timeout,TimeUnit unit) {
		if(holdedLock.get() == null) {
			holdedLock.set(new ArrayList<>());
		}
		if(holdedLock.get().contains(this)) {
			this.innerLock.value = holdedLock.get().get(holdedLock.get().indexOf(this)).getRedLockValue();
			return true;
		}else {
			if(innerLock.tryLock(timeout,unit)) {
				holdedLock.get().add(this);
				return true;
			}
			return false;
		}
	}
	
	public void unlock() {
		innerLock.unlock();
		holdedLock.get().remove(this);
	}
	
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof ReentrantRedLock)) return false;
		ReentrantRedLock that = (ReentrantRedLock) o;
		if(this.innerLock.getLockKey().equals(that.innerLock.getLockKey())) return true;
		return false;
	}
	
	static final class RedisLock {
		
		private static Logger log = LoggerFactory.getLogger("RedisLock");
		
		private StringRedisTemplate stringRedisTemplate;
		private String lockKey;// 竞争资源的标志
		private String value;
		private static final String PREFIX_KEY = "txc_redlock_";
		private boolean locked = false;
		/**
		 * 锁超时时间，防止线程在入锁以后，无限的执行等待
		 */
		private int expireSecond = 60;// 秒

		private static final int DEFAULT_RETRY_INTERVAL_MILLIS_SED = 2;// 毫秒


		private RedisLock(StringRedisTemplate stringRedisTemplate, String lockKey, int expireSecond) {
			this.stringRedisTemplate = stringRedisTemplate;
			this.lockKey = PREFIX_KEY + lockKey;
			this.expireSecond = expireSecond;
		}

		/**
		 * 获取锁
		 * 
		 * @return
		 */
		private boolean lock() {
			log.debug("begin redisLock lock");
			long lockTimeout = getCurrentTimeMillisFromRedis() + expireSecond * 1000 + 1;// 锁时间
			String strLockTimeout = String.valueOf(lockTimeout);
			if (setNX(lockKey, strLockTimeout, expireSecond)) {// 通过SETNX试图获取一个lock SETNX成功，则成功获取一个锁
				this.locked = true;
				this.value = strLockTimeout;
				log.debug("setNX成功");
				log.debug("end redisLock lock");
				return true;
			}
			String strCurrentLockTimeout = stringRedisTemplate.opsForValue().get(lockKey); // 获取redis里面的时间
			log.debug("lockKey:{},strCurrentLockTimeout:{}", lockKey, strCurrentLockTimeout);
			if (strCurrentLockTimeout != null && Long.parseLong(strCurrentLockTimeout) < getCurrentTimeMillisFromRedis()) { //锁已经失效
				log.debug("锁已过期！");
				// 判断是否为空，不为空时，说明已经失效，如果被其他线程设置了值，则第二个条件判断无法执行
				// 获取上一个锁到期时间，并设置现在的锁到期时间
				String strOldLockTimeout = stringRedisTemplate.opsForValue().getAndSet(lockKey, strLockTimeout);
				if (strOldLockTimeout != null && strOldLockTimeout.equals(strCurrentLockTimeout)) {
					log.debug("重新抢到锁");
					// 多线程运行时，多个线程签好都到了这里，但只有一个线程的设置值和当前值相同，它才有权利获取锁
					// 设置超时间，释放内存
					stringRedisTemplate.expire(lockKey, expireSecond * 1000, TimeUnit.MILLISECONDS);
					this.value = strLockTimeout;
					this.locked = true;
					log.debug("end redisLock lock");
					return true;
				}
			}
			log.debug("未抢到锁");
			log.debug("end redisLock lock");
			return false;
		}

		/**
		 * 获取锁
		 * @param timeout
		 * @param unit
		 * @return
		 */
		private boolean tryLock(long timeout, TimeUnit unit) {
			long nanosTimeout = unit.toNanos(timeout);
			final long deadline = System.nanoTime() + nanosTimeout;
			do {
				if (lock()) {
					return true;
				}
				try {
					Thread.sleep(DEFAULT_RETRY_INTERVAL_MILLIS_SED * randInt(0, 6));
					nanosTimeout = deadline - System.nanoTime();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} while (nanosTimeout > 0);
			return false;
		}

		/**
		 * 释放锁
		 */
		private void unlock() {
			log.debug("begin redisLock unlock");
			if (this.locked) {
				String strCurrentLockTimeout = stringRedisTemplate.opsForValue().get(lockKey);// 获取redis里面的时间
				if (strCurrentLockTimeout == null) {// redis已清了
					log.debug("锁已不存在了");
				} else {
					Long currentTimeMillisFromRedis = getCurrentTimeMillisFromRedis();// 还未过期，且是自己加锁，删除之
					if (currentTimeMillisFromRedis > Long.parseLong(strCurrentLockTimeout)) {//过期了
						log.debug("锁已经到期了，不做任何处理。currentTimeMillisFromRedis:{},strCurrentLockTimeout:{}",
								currentTimeMillisFromRedis, strCurrentLockTimeout);
					} else {
						log.debug("锁未过期!");
						if (this.value.equals(strCurrentLockTimeout)) {
							log.debug("锁确定是自己的,可以删除。");
							stringRedisTemplate.delete(lockKey);
						} else {
							log.debug("锁内容不是自己放的，当前不是自己的锁，不执行删除。");
						}
					}
				}
				this.locked = false;

			} else {
				log.debug("原来就没锁住");
			}
			log.debug("end redisLock unlock");
		}

		private boolean setNX(final String key, final String value, final long second) {
			return (Boolean) stringRedisTemplate.execute(new RedisCallback<Boolean>() {
				public Boolean doInRedis(RedisConnection connection) {
					byte[] keyBytes = stringRedisTemplate.getStringSerializer().serialize(key);
					boolean locked = connection.setNX(keyBytes, stringRedisTemplate.getStringSerializer().serialize(value));
					if (locked && second > 0L) {
						connection.expire(keyBytes, second);
					}
					return locked;
				}
			});
		}

		/**
		 * 获取redis时间，避免多服务的时间不一致问题
		 * 
		 * @return
		 */
		private long getCurrentTimeMillisFromRedis() {
			return stringRedisTemplate.execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
					return redisConnection.time();
				}
			});
		}

		/**
		 * 获取锁的名称
		 * 
		 * @return
		 */
		private String getLockKey() {
			return this.lockKey;
		}

		/**
		 * return int in (n,m]
		 * 
		 * @param n
		 * @param m
		 * @return
		 */
		private static int randInt(int n, int m) {
			int w = m - n;
			return (int) Math.ceil(Math.random() * w + n);
		}
	}
}
