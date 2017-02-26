/**
 * 
 */
package com.neu.pdp.pageRank.topK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

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
	 
	public void reduce(NullWritable key, Iterable<CondensedNode> values,
            Context context
            ) throws IOException, InterruptedException {		
		for (CondensedNode cn : values) {			
			pqNodes.add(
					new CondensedNode(
							new Text(cn.getName()),
							new DoubleWritable(cn.getRank().get())));
			
			if (pqNodes.size() > 100) {
				pqNodes.poll();
			}			
		}
		
		List<CondensedNode> pages = new ArrayList<CondensedNode>();
		
		while (!pqNodes.isEmpty()) {
			pages.add(pqNodes.poll());
		}
		
		for (int i = pages.size() - 1; i >= 0 ; i--) {
			CondensedNode node = pages.get(i);
			context.write(node.getName(), node.getRank());
		}
	}
}
