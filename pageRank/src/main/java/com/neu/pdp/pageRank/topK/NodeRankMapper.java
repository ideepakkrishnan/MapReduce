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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.neu.pdp.resources.CondensedNode;

/**
 * @author ideepakkrishnan
 *
 */
public class NodeRankMapper extends Mapper<Object, Text, NullWritable, CondensedNode> {
	
	// Class level variables
	private TreeMap<DoubleWritable, Text> tmRankToNode;
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		// Initialize a TreeMap to store the top 100 pages
		// having highest page ranks. This in-mapper
		// aggregation will reduce the data being passed
		// down into the reducer.
		tmRankToNode = new TreeMap<DoubleWritable, Text>();
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
			
			tmRankToNode.put(dwPageRank, txtPageName);
			
			if (tmRankToNode.size() > 100) {
				tmRankToNode.remove(tmRankToNode.firstKey());
			}
		}
	}
	
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		for (Map.Entry<DoubleWritable, Text> entry 
				: tmRankToNode.entrySet()) {
			context.write(
					NullWritable.get(), 
					new CondensedNode(
							entry.getValue(), entry.getKey()));
		}
	}

}
