package com.sc4.analytics.RIVExamples.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * <p>Creates an MD5 Hash of a string</p>
 * @author Steve Carton (stephen.carton@gmail.com) 
 * Jul 11, 2017
 *
 */
public class MD5 {
	public static String hash(String k) {
		return hash(k, 16);
	}
	public static String hash(String k, int size) {
		MessageDigest hasher;
		try {
			hasher = MessageDigest.getInstance("MD5");
			byte[] kb =k.getBytes();
			byte[] kbh = hasher.digest(kb);
			String bs = new BigInteger(1,kbh).toString(size);
			return bs;
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

}
