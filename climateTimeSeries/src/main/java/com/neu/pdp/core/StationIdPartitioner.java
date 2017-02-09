/**
 * 
 */
package com.neu.pdp.core;

import org.apache.hadoop.mapreduce.Partitioner;

import com.neu.pdp.resources.IntTriplet;
import com.neu.pdp.resources.KeyPair;

/**
 * @author ideepakkrishnan
 *
 */
public class StationIdPartitioner extends Partitioner<KeyPair, IntTriplet> {

	@Override
	public int getPartition(KeyPair key, IntTriplet value, int numPartitions) {
		return Math.abs(key.getStationId().hashCode() * 163) % numPartitions;
	}

}
