/**
 * 
 */
package com.neu.pdp.pageRank.topK;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.neu.pdp.resources.CondensedNode;

/**
 * Mapper class for Top-K
 * The methods in this class finds the local winners for
 * each map task and forwards it into a single reducer
 * (specified during job initialization) so that this
 * reducer can find the global winners.
 * @author ideepakkrishnan
 */
public class NodeRankMapper extends Mapper<Object, Text, NullWritable, CondensedNode> {
	
	// Class level variables
	private PriorityQueue<CondensedNode> pqNodes;
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		// Initialize a priority queue to store the top 100 pages
		// having highest page ranks. This in-mapper
		// aggregation will reduce the data being passed
		// down into the reducer.
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
	
	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		// Local variables
		DoubleWritable dwPageRank = new DoubleWritable(-1);
		String strLine = value.toString();
		
		if (strLine != null && strLine.length() > 0) {
			// Split the record and fetch page name and
			// page rank
			String strLineSplit[] = strLine.split("\t");
			Text txtPageName = new Text(strLineSplit[0]);
				
			dwPageRank = new DoubleWritable(
					Double.parseDouble(
							strLineSplit[1].split(":")[0]));
			
			// Add the entry into priority queue
			pqNodes.add(new CondensedNode(txtPageName, dwPageRank));
			
			if (pqNodes.size() > 100) {
				// Means that we need to remove the
				// page with smallest rank
				pqNodes.poll();
			}
		}
	}
	
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		// Pass on the local winners to reducer
		CondensedNode cn;
		while ((cn = pqNodes.poll()) != null) {
			context.write(NullWritable.get(), cn);
		}
	}

}
