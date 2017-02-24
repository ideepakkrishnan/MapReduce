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
		double dIncomingRanks = 0;
		Node currNode = null;
		double dPageCount = context.getConfiguration().getDouble("totalPages", -1);
		double dAlpha = context.getConfiguration().getDouble("alpha", -1);
		
		for (Node n : values) {
			if (n.getAdjacencyList() == null) {
				dIncomingRanks += n.getPageRank().get();
			} else {
				currNode = n;
			}
		}
		
		if (dPageCount != -1 && dAlpha != -1 && currNode != null) {
			double dNewPageRank = (dAlpha / dPageCount) + 
					(1 - dAlpha) * dIncomingRanks;
			currNode.setPageRank(new DoubleWritable(dNewPageRank));
			
			String sVal = String.valueOf(dNewPageRank) + ":";
			sVal += currNode.getAdjacencyList().toString();
			
			context.write(key, new Text(sVal));
		}
	}
	
}
