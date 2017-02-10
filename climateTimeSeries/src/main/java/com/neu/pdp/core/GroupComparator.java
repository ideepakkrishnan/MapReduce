/**
 * 
 */
package com.neu.pdp.core;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.neu.pdp.resources.KeyPair;
import com.neu.pdp.resources.Util;

/**
 * This class acts as the grouping comparator for our
 * temperature time series program. The mapper passes
 * along a composite key having the format: (station id,
 * year). We need to use the station id from this key
 * to make sure that the data is grouped solely based
 * on their station IDs so that each reduce call gets
 * the complete data from the station(s) that it is
 * processing.
 * @author ideepakkrishnan
 */
public class GroupComparator extends WritableComparator {
	
	/**
	 * The default constructor
	 */
	public GroupComparator() {
		super(KeyPair.class, true);
	}
	
	/**
	 * Compares the station IDs of arguments passed in
	 * to this method and returns the result to caller.
	 * @param wc1 A KeyPair object
	 * @param wc2 Another KeyPair object
	 * @return The comparison result as an integer
	 */
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		// Cast the arguments into appropriate KeyPair variables
		KeyPair kp1 = (KeyPair) wc1;
		KeyPair kp2 = (KeyPair) wc2;
		
		return Util.compare(kp1.getStationId(), kp2.getStationId());
	}

}
