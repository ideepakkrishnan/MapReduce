/**
 * 
 */
package com.neu.pdp.pageRank.core.danglerHandler;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.neu.pdp.pageRank.resources.CondensedNode;

/**
 * @author ideepakkrishnan
 *
 */
public class DanglerNameMapper extends Mapper<Object, Text, CondensedNode, CondensedNode> {
	
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
				val = strLineSplit[1];
				
				// We are dealing with a rank
				context.write(
						new CondensedNode(
								new Text(String.valueOf(source.charAt(0))), 
								new DoubleWritable(1)), // 1 -> Page ID - Rank map
						new CondensedNode(
								new Text(source),
								new DoubleWritable(Double.parseDouble(val))));
			} else {
				
				// We are dealing with dangling node list
				context.write(
						new CondensedNode(
								new Text(String.valueOf(source.charAt(0))), 
								new DoubleWritable(2)), // 2 -> Dangling Node
						new CondensedNode(
								new Text(source),
								new DoubleWritable(Double.NEGATIVE_INFINITY)));
				
			}
		}
		
	}

}
