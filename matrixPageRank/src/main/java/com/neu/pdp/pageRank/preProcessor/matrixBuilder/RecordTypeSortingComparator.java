/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.matrixBuilder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.neu.pdp.pageRank.resources.CondensedNode;

/**
 * @author ideepakkrishnan
 *
 */
public class RecordTypeSortingComparator extends WritableComparator {
	
	/**
	 * The default constructor
	 */
	public RecordTypeSortingComparator() {
		super(CondensedNode.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	public int compare(
			WritableComparable wc1, 
			WritableComparable wc2) {
		
		// Cast the arguments into appropriate KeyPair variables
		CondensedNode cn1 = (CondensedNode) wc1;
		CondensedNode cn2 = (CondensedNode) wc2;
		
		return cn1.compareTo(cn2);
		
	}
	
}