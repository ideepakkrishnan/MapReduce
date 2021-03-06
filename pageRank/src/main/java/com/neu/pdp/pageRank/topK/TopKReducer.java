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
 * Reducer class for Top-K
 * Collects local winners from all map tasks and generates
 * the final list of 100 pages with highest page ranks. 
 * @author ideepakkrishnan
 */
public class TopKReducer extends Reducer<NullWritable, CondensedNode, Text, DoubleWritable> {

	// Class level variables
	private PriorityQueue<CondensedNode> pqNodes;
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		// Initialize a priority queue to store the top 100 pages
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
			// Add this page into priority queue
			pqNodes.add(
					new CondensedNode(
							new Text(cn.getName()),
							new DoubleWritable(cn.getRank().get())));
			
			if (pqNodes.size() > 100) {
				// Means that we need to remove the page
				// with lowest rank in the priority queue
				pqNodes.poll();
			}			
		}
		
		// Array to store the final list in descending
		// order of page ranks
		List<CondensedNode> pages = new ArrayList<CondensedNode>();
		
		// Add the pages from the priority queue into
		// the above list
		while (!pqNodes.isEmpty()) {
			pages.add(pqNodes.poll());
		}
		
		// Write each page in the list into output file
		for (int i = pages.size() - 1; i >= 0 ; i--) {
			CondensedNode node = pages.get(i);
			context.write(node.getName(), node.getRank());
		}
	}
}
