package com.tranboot.client.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ThreadPoolUtils {
	
	//TODO 目前只做单库单数据表同部，所以只开一个线程，如果要做多库多数据表同步，可以根据不同的库不同的表对应不同的处理线程
	private static ExecutorService pool = Executors.newFixedThreadPool(1,new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r,"dbsync-thread");
		}
	});
	
	public static void submit(Runnable task) {
		pool.execute(task);
	}
}
