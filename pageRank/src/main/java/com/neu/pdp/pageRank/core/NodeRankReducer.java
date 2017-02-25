/**
 * 
 */
package com.neu.pdp.pageRank.core;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.neu.pdp.resources.Node;

/**
 * @author ideepakkrishnan
 *
 */
public class NodeRankReducer extends Reducer<Text, Node, Text, Text> {
	
	public void reduce(Text key, Iterable<Node> values,
            Context context
            ) throws IOException, InterruptedException {
		// Local variables
		double dIncomingRanks = 0;
		Node currNode = null;
		
		double dPageCount = context.getConfiguration().getDouble("totalPages", -1);
		double dAlpha = context.getConfiguration().getDouble("alpha", -1);
		
		// Accumulate incoming fractional ranks for current page		
		for (Node n : values) {
			if (n.getType().toString().equals("F")) {
				dIncomingRanks += n.getPageRank().get();
			} else {
				currNode = n;
			}
		}
		
		// Calculate the new page rank for current node
		if (dPageCount != -1 && dAlpha != -1 && currNode != null) {
			
			// New page rank
			double dNewPageRank = (dAlpha / dPageCount) + 
					(1 - dAlpha) * dIncomingRanks;
			
			// Update this new page rank inside the node
			currNode.setPageRank(new DoubleWritable(dNewPageRank));
			
			// Write this out into the output file
			String sVal = String.valueOf(dNewPageRank) + ":";
			sVal += currNode.getAdjacencyList().toString();
			
			context.write(key, new Text(sVal));
		}
	}
	
}
