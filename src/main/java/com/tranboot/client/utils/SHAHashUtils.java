package com.tranboot.client.utils;

import java.io.UnsupportedEncodingException;

import com.google.common.base.Charsets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class SHAHashUtils {
	public static Hasher createHasher() {
		return Hashing.sha256().newHasher();
	}
	
	public static long longHash(Object...objects) {
		Hasher haser = SHAHashUtils.createHasher();
		for(Object o : objects) {
			if(o instanceof Integer) {
				haser.putInt((Integer)o);
			}else if(o instanceof String) {
				haser.putString((String)o, Charsets.UTF_8);
			}else if(o instanceof Long) {
				haser.putLong((Long)o);
			}else if(o instanceof Boolean) {
				haser.putBoolean((Boolean)o);
			}else if(o instanceof Character) {
				haser.putChar((Character)o);
			}else if(o instanceof byte[]) {
				haser.putBytes((byte[])o);
			}else if(o instanceof Byte) {
				haser.putByte((Byte)o);
			}
		}
		return haser.hash().asLong();
	}
	
	public static long unsignedLongHash(Object...objects) {
		Hasher haser = SHAHashUtils.createHasher();
		for(Object o : objects) {
			if(o instanceof Integer) {
				haser.putInt((Integer)o);
			}else if(o instanceof String) {
				haser.putString((String)o, Charsets.UTF_8);
			}else if(o instanceof Long) {
				haser.putLong((Long)o);
			}else if(o instanceof Boolean) {
				haser.putBoolean((Boolean)o);
			}else if(o instanceof Character) {
				haser.putChar((Character)o);
			}else if(o instanceof byte[]) {
				haser.putBytes((byte[])o);
			}else if(o instanceof Byte) {
				haser.putByte((Byte)o);
			}
		}
		return Integer.toUnsignedLong(haser.hash().asInt());
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		System.out.println(SHAHashUtils.unsignedLongHash(7,"editCartItem"));
	}
}
