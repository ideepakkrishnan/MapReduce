/**
 * 
 */
package com.neu.pdp.core;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.neu.pdp.resources.KeyPair;
import com.neu.pdp.resources.Util;

/**
 * This class acts as the sorting comparator for temperature
 * time series program. It sorts the data in such a way that
 * all readings are ordered by their station IDs in the
 * increasing order of their year.
 * @author ideepakkrishnan
 */
public class KeyComparator extends WritableComparator {
	
	/**
	 * The default constructor
	 */
	public KeyComparator() {
		super(KeyPair.class, true);
	}
	
	/**
	 * A function which compares two arguments of type
	 * KeyPair and returns the result as an integer. The
	 * comparison takes place in two steps:
	 *  1. Comparing the Station ID
	 *  2. Comparing the Year
	 * As soon a difference is encountered, the method
	 * returns the corresponding result to the caller.
	 * @param wc1 A KeyPair variable
	 * @param wc2 Another KeyPair variable
	 * @return An integer as result
	 */
	@SuppressWarnings("rawtypes")
	public int compare(
			WritableComparable wc1, 
			WritableComparable wc2) {
		// Since we are expecting KeyPair objects as arguments,
		// cast the arguments into proper variables
		KeyPair kp1 = (KeyPair) wc1;
		KeyPair kp2 = (KeyPair) wc2;
		
		int cmp = Util.compare(kp1.getStationId(), kp2.getStationId());
		
		if (cmp != 0) {
			return cmp;
		}
		
		return Util.compare(kp1.getYear(), kp2.getYear());
		
	}
	
}
