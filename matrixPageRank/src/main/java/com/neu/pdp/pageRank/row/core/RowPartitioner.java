/**
 * 
 */
package com.neu.pdp.pageRank.row.core;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.neu.pdp.pageRank.resources.SourceRankPair;

/**
 * @author ideepakkrishnan
 *
 */
public class RowPartitioner extends Partitioner<LongWritable, SourceRankPair> {

	@Override
	public int getPartition(LongWritable key, SourceRankPair value, int numPartitions) {
		try {
			return Math.toIntExact(key.get()) % numPartitions;
		} catch (Exception e) {
			return key.hashCode() % numPartitions;
		}
	}

}
