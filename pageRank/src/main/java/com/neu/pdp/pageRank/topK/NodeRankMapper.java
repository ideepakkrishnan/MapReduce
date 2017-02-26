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
 * @author ideepakkrishnan
 *
 */
public class NodeRankMapper extends Mapper<Object, Text, NullWritable, CondensedNode> {
	
	// Class level variables
	private PriorityQueue<CondensedNode> pqNodes;
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		// Initialize a TreeMap to store the top 100 pages
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
			// Split the record and fetch page name
			String strLineSplit[] = strLine.split("\t");
			Text txtPageName = new Text(strLineSplit[0]);
				
			dwPageRank = new DoubleWritable(
					Double.parseDouble(
							strLineSplit[1].split(":")[0]));
			
			pqNodes.add(new CondensedNode(txtPageName, dwPageRank));
			
			if (pqNodes.size() > 100) {
				pqNodes.poll();
			}
		}
	}
	
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		CondensedNode cn;
		while ((cn = pqNodes.poll()) != null) {
			context.write(NullWritable.get(), cn);
		}
	}

}
