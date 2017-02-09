/**
 * 
 */
package com.neu.pdp.core;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.neu.pdp.resources.KeyPair;

/**
 * @author ideepakkrishnan
 *
 */
public class StationIdPartitioner extends Partitioner<KeyPair, NullWritable> {

	@Override
	public int getPartition(KeyPair key, NullWritable value, int numPartitions) {		
		return Math.abs(key.getStationId().hashCode() * 163) % numPartitions;
	}

}
