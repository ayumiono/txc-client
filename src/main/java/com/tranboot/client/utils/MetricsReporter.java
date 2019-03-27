package com.tranboot.client.utils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;
import com.tranboot.client.core.txc.TxcDataSourceTransactionManagerInterceptor;
import com.tranboot.client.core.txc.TxcDubboFilter;
import com.tranboot.client.core.txc.TxcMethodInterceptor;
import com.tranboot.client.model.txc.ManualRollbackTxcSQL;
import com.tranboot.client.model.txc.SqlParserTxcSQL;
import com.tranboot.client.model.txc.TxcSqlParserProcessor;
import com.tranboot.client.service.txc.TxcRedisService;
import com.tranboot.client.service.txc.impl.TxcManualRollbackSQLServiceMysqlImpl;

public class MetricsReporter {
	
	final static MetricRegistry metrics = new MetricRegistry();
	final static JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
	
	//重要指标
	public final static Counter txcTimeoutCounter = MetricsReporter.register("txcTimeoutCounter", new Counter());
	public final static  Meter throughput = MetricsReporter.register("qps", new Meter());
	
	
	public final static Counter redLockFailCount = MetricsReporter.register("redLockFailCount", new Counter());
	public final static Timer redLockTimer = MetricsReporter.timer("redlock");
	public final static Timer redUnlockTimer = MetricsReporter.timer("redUnlock");
	
	public static final Timer hput = MetricsReporter.timer(MetricRegistry.name(TxcRedisService.class, "hput"));
	public static final Timer time = MetricsReporter.timer(MetricRegistry.name(TxcRedisService.class, "redis_time"));
	
	//次要指标
	public static final Timer commitTimer = MetricsReporter.timer(MetricRegistry.name(TxcDataSourceTransactionManagerInterceptor.class, "doCommit"));
	public static final Timer rollbackTimer = MetricsReporter.timer(MetricRegistry.name(TxcDataSourceTransactionManagerInterceptor.class, "doRollback"));
	public static final Timer beginTimer = MetricsReporter.timer(MetricRegistry.name(TxcDataSourceTransactionManagerInterceptor.class, "doBegin"));
	
	public static final Timer sendCommitMessage = MetricsReporter.timer(MetricRegistry.name(TxcMethodInterceptor.class, "sendCommitMessage"));
	public static final Timer sendRollbackMessage = MetricsReporter.timer(MetricRegistry.name(TxcMethodInterceptor.class, "sendRollbackMessage"));
	
	public static final Timer txcSqlCacheTimer = MetricsReporter.timer(MetricRegistry.name(LRUCache.class, "getTxcSql"));
	public static final Timer dbsyncCacheTimer = MetricsReporter.timer(MetricRegistry.name(LRUCache.class, "getDbsyncSqlTransformResult"));
	public static final Timer schemaCacheTimer = MetricsReporter.timer(MetricRegistry.name(LRUCache.class, "getTableSchema"));
	public static final Timer txcSQLTransformTimer = MetricsReporter.timer(MetricRegistry.name(LRUCache.class, "getTxcSqlTransform"));
	
	
	public static final Timer parserProcessorTimer = MetricsReporter.timer(MetricRegistry.name(TxcSqlParserProcessor.class, "parse"));
	public static final Timer rollbackSqlTimer = MetricsReporter.timer(MetricRegistry.name(SqlParserTxcSQL.class, "rollbackSql"));
	public static final Timer query = MetricsReporter.timer(MetricRegistry.name(SqlParserTxcSQL.class, "query"));
	public static final Timer render = MetricsReporter.timer(MetricRegistry.name(SqlParserTxcSQL.class, "render"));
	public static final Timer deal = MetricsReporter.timer(MetricRegistry.name(SqlParserTxcSQL.class, "deal"));
	public static final Timer manualRollbackSqlTimer = MetricsReporter.timer(MetricRegistry.name(ManualRollbackTxcSQL.class, "rollbackSql"));
	
	public static final Timer insertTxcLog = MetricsReporter.timer(MetricRegistry.name(TxcMethodInterceptor.class, "insertTransactionLog"));
	public static final Timer queryManualRollbackSql = MetricsReporter.timer(MetricRegistry.name(TxcManualRollbackSQLServiceMysqlImpl.class, "queryManualRollbackSql"));
	public static final Timer invokeTimer = MetricsReporter.timer(MetricRegistry.name(TxcDubboFilter.class, "invoke"));
	
	static {
		reporter.start();
	}

	public static Timer timer(String name) {
		return metrics.timer(name);
	}
	
	public static <T extends Metric> T register(String name, T metric) {
		return metrics.register(name, metric);
	}
}
