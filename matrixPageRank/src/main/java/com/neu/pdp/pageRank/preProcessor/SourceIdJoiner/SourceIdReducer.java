/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.SourceIdJoiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author ideepakkrishnan
 *
 */
public class SourceIdReducer extends Reducer<Text, Text, Text, Text> {
	
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
		
		
		context.write(new Text(source), new Text(outlinks));
	}

}
