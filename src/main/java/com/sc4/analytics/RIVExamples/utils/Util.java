package com.sc4.analytics.RIVExamples.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Misc static Utilities.
 * 
 * @author Steve Carton (stephen.carton@gmail.com) Jul 14, 2017
 *
 */
public class Util {
	final static Logger logger = LoggerFactory.getLogger(Util.class);

	private Util() {}

	/**
	 * makes a String out of a JAVA stack trace
	 * @param e
	 *            Exception to trace
	 * @return resulting trace as a String
	 */
	public static String stackTrace(Exception e) {
		if (e == null)
			return ("No StackTrace available on NULL exception.");
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}
}
