/**
 * 
 */
package com.neu.pdp.pageRank.row.preProcessor.SourceIdJoiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @author ideepakkrishnan
 *
 */
public class SourceMapper extends Mapper<Object, Text, Text, Text> {
	
	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		String source = "";
		String val = "";
		
		String strLine = value.toString();
		
		if (strLine != null && strLine.length() > 0) {
			// Split the record and fetch page name
			String strLineSplit[] = strLine.split("\t");
			source = strLineSplit[0];
			
			if (strLineSplit.length > 1) {
				// Current page is not a dangling node
				// or the value is an ID
				val = strLineSplit[1];
			}
			
			context.write(new Text(source), new Text(val));
		}
	}

}
