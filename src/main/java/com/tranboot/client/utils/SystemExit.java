package com.tranboot.client.utils;

public class SystemExit {
	public static void exit(String msg) {
		System.out.println("--------------------------------->"+msg+"<---------------------------------------");
		System.exit(9);
	}
	public static void sleep(long timeout,String msg) {
		System.out.println("--------------------------------->"+msg+"开始睡眠"+timeout/1000+"<---------------------------------------");
		try {
			Thread.sleep(timeout);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("--------------------------------->"+msg+"结束睡眠<---------------------------------------");
	}
}

