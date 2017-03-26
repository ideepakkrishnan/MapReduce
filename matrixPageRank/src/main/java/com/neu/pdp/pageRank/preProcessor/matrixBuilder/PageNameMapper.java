/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.matrixBuilder;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.neu.pdp.pageRank.resources.SourceRankPair;

/**
 * @author ideepakkrishnan
 *
 */
public class PageNameMapper extends Mapper<Object, Text, Text, SourceRankPair> {
	
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
				if (val.startsWith("/")) {
					// This records is a page name - ID map
					context.write(
							new Text(source), 
							new SourceRankPair(
									new LongWritable(
											Long.parseLong(val.substring(1))),
									new DoubleWritable(
											Double.NEGATIVE_INFINITY)));
				} else {
					val = strLineSplit[1];
					String[] outlinks = val.split(",");
					Long pageCount = context.getConfiguration().getLong("pageCount", 0);
					Double defaultPageRank = 1 / (double) pageCount;
					for (String s : outlinks) {
						context.write(
								new Text(s), 
								new SourceRankPair(
										new LongWritable(Long.parseLong(source)), 
										new DoubleWritable(defaultPageRank)));
					}
				}				
			}
		}
		
	}

}
