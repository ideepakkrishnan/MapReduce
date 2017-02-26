/**
 * 
 */
package com.neu.pdp.pageRank.topK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
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
	private PriorityQueue<CondensedNode> pqNodes;
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		// Initialize a TreeMap to store the top 100 pages
		// having highest page ranks.
		pqNodes = new PriorityQueue<CondensedNode>(
				new Comparator<CondensedNode>() {
					public int compare(CondensedNode o1, CondensedNode o2) {
						if (o1.getRank().get() > o2.getRank().get()) {
							return 1;
						} else if (o1.getRank().get() < o2.getRank().get()) {
							return -1;
						} else {
							return 0;
						}
					}
				});
	}
	
	public void reduce(NullWritable key, Iterable<CondensedNode> values,
            Context context
            ) throws IOException, InterruptedException {		
		for (CondensedNode cn : values) {
			pqNodes.add(new CondensedNode(cn.getName(), cn.getRank()));
			if (pqNodes.size() > 100) {
				pqNodes.poll();
			}
		}
		
		CondensedNode node;
		List<CondensedNode> pages = new ArrayList<CondensedNode>();
		
		while (!pqNodes.isEmpty()) {
			node = pqNodes.poll();
			pages.add(node);
		}
		
		for (int i = pages.size() - 1; i >= 0 ; i--) {
			node = pages.get(i);
			context.write(node.getName(), node.getRank());
		}
	}
}
