/**
 * 
 */
package com.neu.pdp.core;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.neu.pdp.resources.KeyPair;

/**
 * @author ideepakkrishnan
 *
 */
public class GroupComparator extends WritableComparator {
	
	public GroupComparator() {
		super(KeyPair.class, true);
	}
	
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		// Cast the arguments into appropriate KeyPair variables
		KeyPair kp1 = (KeyPair) wc1;
		KeyPair kp2 = (KeyPair) wc2;
		
		return kp1.compare(kp1.getStationId(), kp2.getStationId());
	}

}
