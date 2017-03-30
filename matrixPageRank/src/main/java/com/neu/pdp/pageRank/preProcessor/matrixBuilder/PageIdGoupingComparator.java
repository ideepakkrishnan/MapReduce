/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.matrixBuilder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.neu.pdp.pageRank.resources.CondensedNode;
import com.neu.pdp.pageRank.resources.KeyPair;
import com.neu.pdp.pageRank.resources.Util;

/**
 * This class groups the incoming records primarily based on
 * the first letter of the page name and secondarily based
 * on the type of record [1: Page ID, 2: Outlink]. It makes
 * sure that all the records who page names start with a
 * particular letter end up in the same reducer call.
 * @author ideepakkrishnan
 */
public class PageIdGoupingComparator extends WritableComparator {
	
	/**
	 * The default constructor
	 */
	public PageIdGoupingComparator() {
		super(CondensedNode.class, true);
	}
	
	/**
	 * Compares the first letter of page names passed in as
	 * argument and if they are the same, a secondary
	 * comparison is performed on the type of record that
	 * is specified.
	 * @param wc1 A CondensedNode object
	 * @param wc2 Another CondensedNode object
	 * @return The comparison result as an integer
	 */
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		// Cast the arguments into appropriate KeyPair variables
		CondensedNode cn1 = (CondensedNode) wc1;
		CondensedNode cn2 = (CondensedNode) wc2;
		
		return cn1.getName().compareTo(cn2.getName());
	}

}
