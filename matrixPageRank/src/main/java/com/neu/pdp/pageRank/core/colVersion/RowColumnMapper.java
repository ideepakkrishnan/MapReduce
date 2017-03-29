/**
 * 
 */
package com.neu.pdp.pageRank.core.colVersion;

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
public class RowColumnMapper extends Mapper<Object, Text, LongWritable, SourceRankPair> {
	
	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		
		Long col;
		String val = "";
		
		String strLine = value.toString();
		
		if (strLine != null && strLine.length() > 0) {
			// Split the record and fetch page name
			String strLineSplit[] = strLine.split("\t");
			col = Long.parseLong(strLineSplit[0]);
			
			// Safety check
			if (strLineSplit.length > 1) {
				val = strLineSplit[1];
				
				String[] rowAndVals = val.split(",");
				
				for (String element : rowAndVals) {
					String[] rowAndVal = element.split(":");
					Long row = Long.parseLong(rowAndVal[0]);
					double cj = Double.parseDouble(rowAndVal[1]);
					
					SourceRankPair p = new SourceRankPair(
							new Text(""), 
							new LongWritable(col), 
							new DoubleWritable(cj));
					
					context.write(new LongWritable(row), p);
				}
			}
		}
		
	}

}
