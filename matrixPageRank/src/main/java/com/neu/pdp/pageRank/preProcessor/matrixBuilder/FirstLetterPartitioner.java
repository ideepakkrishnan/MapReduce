/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.matrixBuilder;

import org.apache.hadoop.mapreduce.Partitioner;

import com.neu.pdp.pageRank.resources.CondensedNode;
import com.neu.pdp.pageRank.resources.SourceRankPair;

/**
 * This class acts as a partitioner for the third step
 * in pre-processor. It partitions the records based
 * on the first letter of the page name so that each
 * worker is processing words starting with the same
 * letter. 
 * @author ideepakkrishnan
 */
public class FirstLetterPartitioner extends Partitioner<CondensedNode, SourceRankPair> {

	@Override
	public int getPartition(CondensedNode key, SourceRankPair value, int numPartitions) {
		return Math.abs(key.getName().charAt(0)) % numPartitions;
	}

}
