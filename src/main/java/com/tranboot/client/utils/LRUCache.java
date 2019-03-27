package com.tranboot.client.utils;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.tranboot.client.exception.TxcTransactionException;
import com.tranboot.client.model.TableSchema;
import com.tranboot.client.model.dbsync.SqlTransformResult;
import com.tranboot.client.model.txc.TxcSQL;
import com.tranboot.client.model.txc.TxcSQLTransformer;
import com.tranboot.client.model.txc.TxcSQLTransformer.TxcSQLTransform;
import com.tranboot.client.service.TableSchemaCacheLoader;
import static com.tranboot.client.utils.MetricsReporter.*;

/**
 * LRU 缓存
 * @author xuelong.chen
 */
public class LRUCache {
	
	private static final Logger logger = LoggerFactory.getLogger(LRUCache.class);
	/**
	 * 如果需要输出成日志，需要配置LRUmonitor的appender
	 */
	private static final Logger monitor = LoggerFactory.getLogger("LRUmonitor");
	
	private static final Cache<String, TxcSQL> txcSqlCache;
	private static final Cache<String, TableSchema> tableSchemaCache;
	private static final Cache<String, SqlTransformResult> dbsyncSqlTransformCache;
	private static final Cache<String, TxcSQLTransform> txcSQLTransformCache;
	
	static class CacheStatusMetric implements Gauge<String> {
		@SuppressWarnings("rawtypes")
		Cache cache;
		@SuppressWarnings("rawtypes")
		public CacheStatusMetric(Cache cache) {
			this.cache = cache;
		}

		@Override
		public String getValue() {
			return this.cache.stats().toString();
		}
	}	
	
	static {
		txcSqlCache = CacheBuilder.newBuilder()
				.concurrencyLevel(10)
				.initialCapacity(100)
				.maximumSize(200)
				.removalListener(new RemovalListener<String, TxcSQL>() {
					@Override
					public void onRemoval(RemovalNotification<String, TxcSQL> notification) {
						logger.info(notification.getKey() + " txc 缓存过期，将被移除");
					}
				})
				.recordStats()
				.build();
		MetricsReporter.register(MetricRegistry.name(LRUCache.class, "txcSqlCache"), new CacheStatusMetric(txcSqlCache));
		tableSchemaCache = CacheBuilder.newBuilder()
				.concurrencyLevel(10)
				.initialCapacity(50)
				.maximumSize(100)
				.recordStats()
				.removalListener(new RemovalListener<String, TableSchema>() {
					@Override
					public void onRemoval(RemovalNotification<String, TableSchema> notification) {
						logger.info(notification.getKey() + " shcema 缓存过期,将被移除");
					}
				})
				.build();
		MetricsReporter.register(MetricRegistry.name(LRUCache.class, "tableSchemaCache"), new CacheStatusMetric(tableSchemaCache));
		dbsyncSqlTransformCache = CacheBuilder.newBuilder()
				.concurrencyLevel(10)
				.initialCapacity(100)
				.maximumSize(200)
				.recordStats()
				.removalListener(new RemovalListener<String, SqlTransformResult>() {
					@Override
					public void onRemoval(RemovalNotification<String, SqlTransformResult> notification) {
						logger.info(notification.getKey() + " dbsync sqltransformresult 缓存过期,将被移除");
					}
				})
				.build();
		MetricsReporter.register(MetricRegistry.name(LRUCache.class, "dbsyncSqlTransformCache"), new CacheStatusMetric(dbsyncSqlTransformCache));
	
		txcSQLTransformCache = CacheBuilder.newBuilder()
				.concurrencyLevel(10)
				.initialCapacity(100)
				.maximumSize(200)
				.recordStats()
				.removalListener(new RemovalListener<String, TxcSQLTransform>() {
					@Override
					public void onRemoval(RemovalNotification<String, TxcSQLTransform> notification) {
						logger.info(notification.getKey() + " txcsqltransform 缓存过期,将被移除");
					}
				})
				.build();
		MetricsReporter.register(MetricRegistry.name(LRUCache.class, "txcSQLTransformCache"), new CacheStatusMetric(txcSQLTransformCache));
	}
	
	public static final void cacheDbsyncSqlTransformResult(String sql,SqlTransformResult result) {
		dbsyncSqlTransformCache.put(sql, result);
	}
	
	public static final TxcSQL getTxcSql(String sql,Callable<TxcSQL> loader) {
		Timer.Context context = txcSqlCacheTimer.time();
		try {
			TxcSQL txcSql = txcSqlCache.get(sql,loader);
			monitor.info(System.lineSeparator()+"原语句:"+sql+System.lineSeparator()+txcSql.toString());
			return txcSql;
		} catch (Exception e) {
			throw new TxcTransactionException(e, "获取TxcSQL失败");
		} finally {
			context.stop();
		}
	}
	
	public static final SqlTransformResult getDbsyncSqlTransformResult(String sql) {
		Timer.Context context = dbsyncCacheTimer.time();
		try {
			return dbsyncSqlTransformCache.getIfPresent(sql);
		} finally {
			context.stop();
		}
	}
	
	public static final TxcSQLTransform getTxcTransformedSql (String sql) {
		Timer.Context context = txcSQLTransformTimer.time();
		try {
			return txcSQLTransformCache.get(sql, new Callable<TxcSQLTransform>() {
				@Override
				public TxcSQLTransform call() throws Exception {
					TxcSQLTransformer transformer = new TxcSQLTransformer();
					TxcSQLTransform transform = transformer.transform(sql);
					return transform;
				}
			});
		} catch (Exception e) {
			throw new TxcTransactionException(e, "获取TxcTransformedSql失败");
		} finally {
			context.stop();
		}
	}
	
	public static final boolean tableSchemaCacheContain(String	tableName) {
		if(tableSchemaCache.getIfPresent(tableName) == null) {
			return false;
		}
		return true;
	}
	
	public static final TableSchema getTableSchema(String tableName,TableSchemaCacheLoader loader) {
		Timer.Context context = schemaCacheTimer.time();
		try {
			TableSchema schema = tableSchemaCache.get(tableName, loader);
			logger.info("TableSchema:{}", schema);
			monitor.info(schema.toString());
			return schema;
		} catch (Exception e) {
			throw new TxcTransactionException(e, "获取TableSchema失败");
		} finally {
			context.stop();
		}
	}
}
