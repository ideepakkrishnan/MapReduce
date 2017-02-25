/**
 * 
 */
package com.neu.pdp.pageRank.topK;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.neu.pdp.resources.CondensedNode;

/**
 * @author ideepakkrishnan
 *
 */
public class TopKReducer extends Reducer<NullWritable, CondensedNode, Text, DoubleWritable> {

	// Class level variables
	private TreeMap<DoubleWritable, Text> tmRankToNode;
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		// Initialize a TreeMap to store the top 100 pages
		// having highest page ranks.
		tmRankToNode = new TreeMap<DoubleWritable, Text>();
	}
	
	public void reduce(NullWritable key, Iterable<CondensedNode> values,
            Context context
            ) throws IOException, InterruptedException {
		for (CondensedNode cn : values) {
			tmRankToNode.put(cn.getRank(), cn.getName());
			
			if (tmRankToNode.size() > 100) {
				tmRankToNode.remove(tmRankToNode.firstKey());
			}
		}
	}
	
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		for (Map.Entry<DoubleWritable, Text> entry 
				: tmRankToNode.entrySet()) {
			context.write(entry.getValue(), entry.getKey());
		}
	}
}
