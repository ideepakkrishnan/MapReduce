/**
 * 
 */
package com.neu.pdp.resources;

import org.apache.hadoop.io.WritableComparable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class for the project which maintains all helper
 * functions
 * @author ideepakkrishnan
 */
public class Util {
	
	private static final Logger logger = LogManager.getLogger(
			Util.class.getName());
	
	/**
	 * Compares two WritableComparable objects with each other
	 * and returns the result to caller
	 * @param w1
	 * @param w2
	 * @return Result of comparison as an integer
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static int compare(WritableComparable w1, WritableComparable w2) {
		logger.info("Executing compare method");
		return w1.compareTo(w2);		
	}
	
}
