/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.SourceIdJoiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author ideepakkrishnan
 *
 */
public class SourceIdReducer extends Reducer<Text, Text, Text, Text> {
	
	private MultipleOutputs<Text, Text> mos;
	private double defaultPageRank;
	private String rankOutputPath;
	private String danglingNodesPath;
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		mos = new MultipleOutputs(context);
		defaultPageRank = 1 / (double) context.getConfiguration()
											  .getLong("totalPageCount", -1);
		rankOutputPath = context.getConfiguration().get("defaultRankPath");
		danglingNodesPath = context.getConfiguration().get("danglingNodesPath");
	}
	
	public void reduce(Text key, Iterable<Text> values,
            Context context
            ) throws IOException, InterruptedException {
		
		String source = "";
		String outlinks = "";
		
		for (Text t : values) {
			String val = t.toString();
			if (val != null && val.length() > 0) {
				if (val.startsWith("/")) {
					// Means that this is the page ID
					source = val.substring(1);
				} else {
					outlinks = val;
				}
			}
		}
		
		Text tSource = new Text(source);
		
		// Write the mapped record (source page ID, adjacency list)
		// to HDFS
		context.write(tSource, new Text(outlinks));
		
		// Write the default page rank for all pages so that it
		// can be used for first iteration of page rank algorithm
		mos.write(tSource, new Text(String.valueOf(defaultPageRank)), rankOutputPath + "/ranks");
		
		// Write dangling nodes into a file
		if (outlinks == null || outlinks.length() == 0 || outlinks.equals("")) {
			mos.write(tSource, new Text(""), danglingNodesPath + "/danglingNodes");
		}
	}
	
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		mos.close();
	}

}
