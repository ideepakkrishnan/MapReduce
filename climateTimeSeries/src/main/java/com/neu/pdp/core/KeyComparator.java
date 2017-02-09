/**
 * 
 */
package com.neu.pdp.core;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.neu.pdp.resources.KeyPair;
import com.neu.pdp.resources.Util;

/**
 * @author ideepakkrishnan
 *
 */
public class KeyComparator extends WritableComparator {
	
	public KeyComparator() {
		super(KeyPair.class, true);
	}
	
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
