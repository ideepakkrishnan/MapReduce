/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.topK;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.neu.pdp.pageRank.resources.SourceRankPair;

/**
 * @author ideepakkrishnan
 *
 */
public class NodeRankMapper extends Mapper<Object, Text, NullWritable, SourceRankPair> {
	
	// Class level variables
	private PriorityQueue<SourceRankPair> pqNodes;
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		// Initialize a priority queue to store the top 100 pages
		// having highest page ranks. This in-mapper
		// aggregation will reduce the data being passed
		// down into the reducer.
		pqNodes = new PriorityQueue<SourceRankPair>(
				new Comparator<SourceRankPair>() {
					public int compare(SourceRankPair o1, SourceRankPair o2) {
						if (o1.getRank().get() < o2.getRank().get()) {
							return -1;
						} else if (o1.getRank().get() > o2.getRank().get()) {
							return 1;
						} else {
							return 0;
						}
					}
				});
		
	}
	
	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		
		LongWritable source;
		DoubleWritable rank;
		
		String[] contents = value.toString().split("\t");
		
		if (contents != null && contents.length == 2) {
			source = new LongWritable(Long.parseLong(contents[0]));
			rank = new DoubleWritable(Double.parseDouble(contents[1]));
			
			// Add this node into the priority queue
			pqNodes.add(new SourceRankPair(new Text(""), source, rank));
			
			// Maintain the size of priority queue
			if (pqNodes.size() > 100) {
				// Remove the smallest node from the queue
				pqNodes.poll();
			}
		}
		
	}
	
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		// Pass on the local winners to the reducer
		SourceRankPair node;
		while ((node = pqNodes.poll()) != null) {
			context.write(NullWritable.get(), node);
		}
	}

}