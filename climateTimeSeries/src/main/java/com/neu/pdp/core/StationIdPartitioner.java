/**
 * 
 */
package com.neu.pdp.core;

import org.apache.hadoop.mapreduce.Partitioner;

import com.neu.pdp.resources.IntTriplet;
import com.neu.pdp.resources.KeyPair;

/**
 * This class makes sure that the data for a particular 
 * station id is forwarded to a reducer task.
 * @author ideepakkrishnan
 */
public class StationIdPartitioner extends Partitioner<KeyPair, IntTriplet> {

	/**
	 * Since the mapper's output key is a composite key,
	 * this method is used to forward the data based on
	 * only the station ID.
	 * @param key Mapper's output key
	 * @param value Mapper's output value
	 * @param numPartitions Number of available partitions
	 * @return Partition to which the data is to be forwarded
	 */
	@Override
	public int getPartition(KeyPair key, IntTriplet value, int numPartitions) {
		return Math.abs(key.getStationId().hashCode() * 163) % numPartitions;
	}

}
