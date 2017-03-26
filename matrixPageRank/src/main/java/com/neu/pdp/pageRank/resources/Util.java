/**
 * 
 */
package com.neu.pdp.pageRank.resources;

import org.apache.hadoop.io.WritableComparable;

/**
 * Utility class for the project which maintains all helper
 * functions
 * @author ideepakkrishnan
 */
public class Util {
	
	/**
	 * Compares two WritableComparable objects with each other
	 * and returns the result to caller
	 * @param w1
	 * @param w2
	 * @return Result of comparison as an integer
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static int compare(WritableComparable w1, WritableComparable w2) {
		return w1.compareTo(w2);
	}
	
}
