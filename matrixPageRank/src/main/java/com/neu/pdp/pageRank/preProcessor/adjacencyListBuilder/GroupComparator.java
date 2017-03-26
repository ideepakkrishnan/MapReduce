/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.adjacencyListBuilder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.neu.pdp.pageRank.resources.KeyPair;
import com.neu.pdp.pageRank.resources.Util;

/**
 * This class acts as the grouping comparator for the
 * pre-processor program. The mapper passes
 * along a composite key having the format: (Text,
 * Text). We need to use the first element in this key
 * to group the data and send it over to an appropriate
 * reduce call.
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
		
		return Util.compare(kp1.getFirst(), kp2.getFirst());
	}

}
