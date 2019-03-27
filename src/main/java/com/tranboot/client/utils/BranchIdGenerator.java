package com.tranboot.client.utils;

import java.util.UUID;

/**
 * 分支事务号生成器
 * @author xuelong.chen
 *
 */
public class BranchIdGenerator {
	
	public static long branchId() {
		return SHAHashUtils.unsignedLongHash(System.currentTimeMillis(),UUID.randomUUID().toString());
	}
}
