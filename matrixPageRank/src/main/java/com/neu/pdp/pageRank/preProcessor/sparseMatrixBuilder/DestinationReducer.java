/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.sparseMatrixBuilder;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author ideepakkrishnan
 *
 */
public class DestinationReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values,
            Context context
            ) throws IOException, InterruptedException {
		
		String val = "";
		
		for (Text t : values) {
			String colEntry = t.toString();
			
			if (colEntry != null && colEntry.length() > 0) {
				if (val.length() == 0) {
					val = colEntry;
				} else {
					val = val + "," + colEntry;
				}
			}
		}
		
		context.write(key, new Text(val));
	}

}
