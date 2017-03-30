/**
 * 
 */
package com.neu.pdp.pageRank.core.danglerHandler;

import org.apache.hadoop.mapreduce.Partitioner;

import com.neu.pdp.pageRank.resources.CondensedNode;

/**
 * @author ideepakkrishnan
 *
 */
public class DanglerIdFirstLetterPartitioner extends Partitioner<CondensedNode, CondensedNode> {

	@Override
	public int getPartition(CondensedNode key, CondensedNode value, int numPartitions) {
		return Math.abs(key.getName().charAt(0)) % numPartitions;
	}

}
