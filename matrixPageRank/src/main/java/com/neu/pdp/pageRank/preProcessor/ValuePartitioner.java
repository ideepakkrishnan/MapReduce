/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.neu.pdp.pageRank.resources.KeyPair;

/**
 * The incoming records have their keys in format:
 * (Text, Text). This class uses the first element
 * in this key to make sure that the data having 
 * same value in this first element is forwarded to 
 * a reducer task.
 * @author ideepakkrishnan
 */
public class ValuePartitioner extends Partitioner<KeyPair, Text> {

	/**
	 * Since the mapper's output key is a composite key,
	 * this method is used to forward the data based on
	 * only the first element in this key.
	 * @param key Mapper's output key
	 * @param value Mapper's output value
	 * @param numPartitions Number of available partitions
	 * @return Partition to which the data is to be forwarded
	 */
	@Override
	public int getPartition(KeyPair key, Text value, int numPartitions) {
		return Math.abs(key.getFirst().hashCode() * 163) % numPartitions;
	}

}
