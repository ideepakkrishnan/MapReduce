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

import com.neu.pdp.pageRank.resources.CondensedNode;
import com.neu.pdp.pageRank.resources.KeyPair;
import com.neu.pdp.pageRank.resources.SourceRankPair;

/**
 * @author ideepakkrishnan
 *
 */
public class PageNameMapper extends Mapper<Object, Text, CondensedNode, SourceRankPair> {
	
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
				
				if (val.startsWith("/")) {
					// This records is a page name - ID map. We
					// need to generate the following record:
					// Key - (E, 1)
					// Value - (England, 15, NegativeInfinity)
					// In the value, second element represents
					// the ID for England.
					context.write(
							new CondensedNode(
									new Text(String.valueOf(source.charAt(0))), 
									new DoubleWritable(1)), // 1 -> Page name - ID map
							new SourceRankPair(
									new Text(source),
									new LongWritable(
											Long.parseLong(val.substring(1))),
									new DoubleWritable(
											Double.NEGATIVE_INFINITY)));
				} else {
					// This record is an adjacency list. We
					// need to generate the following record:
					// Key: (I, 2)
					// Value: (India, 5, 0.74635)
					// In the value, the second element represents
					// the source page's ID.
					String[] outlinks = val.split(",");
					double outlinkCount = outlinks.length;
					double cj = 1 / outlinkCount;
					for (String s : outlinks) {
						context.write(
								new CondensedNode(
										new Text(String.valueOf(s.charAt(0))),
										new DoubleWritable(2)), // 2 -> Outlink representation
								new SourceRankPair(
										new Text(s),
										new LongWritable(Long.parseLong(source)), 
										new DoubleWritable(cj))); // cj = 1 / [source outlink count]
					}
				}
			}
		}
		
	}

}
