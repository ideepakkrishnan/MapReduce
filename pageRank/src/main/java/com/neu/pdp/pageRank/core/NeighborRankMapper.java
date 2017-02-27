/**
 * 
 */
package com.neu.pdp.pageRank.core;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.neu.pdp.resources.Node;
import com.neu.pdp.resources.TextArrayWritable;

/**
 * The mapper class for Page Rank algorithm.
 * @author ideepakkrishnan
 */
public class NeighborRankMapper extends Mapper<Object, Text, Text, Node> {

	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		
		// Local variables
		String strAdjPages = "";
		String arrAdjPages[] = new String[0];
		DoubleWritable dwPageRank = new DoubleWritable(-1);
		double dPageCount = context.getConfiguration()
				.getDouble("totalPages", -1);
		
		String strLine = value.toString();
		
		if (strLine != null && strLine.length() > 0) {
			// Split the record and fetch page name
			String strLineSplit[] = strLine.split("\t");
			String strPageName = strLineSplit[0];
			
			if (strLineSplit.length > 1 && 
					strLineSplit[1].indexOf(':') >= 0) {
				// Means that this records has been processed
				// at least once and hence, we have a page rank
				// available from previous run. Split and get
				// this page rank.
				String strValSplit[] = strLineSplit[1].split(":");
				
				dwPageRank = new DoubleWritable(
						Double.parseDouble(strValSplit[0]));
				
				if (strValSplit != null && strValSplit.length > 1) {
					// Current page has out-links
					strAdjPages = strValSplit[1];
					arrAdjPages = strValSplit[1].split(",");
				}
			} else {
				// Means that this record is being processed
				// by the page rank algorithm for the first
				// time. So, assign the default page rank and
				// continue.
				dwPageRank = new DoubleWritable(1/dPageCount);
				
				if (strLineSplit.length > 1) {
					// Current page has out-links
					strAdjPages = strLineSplit[1];
					arrAdjPages = strLineSplit[1].split(",");
				}
			}
			
			// Calculate the page rank to be split and sent
			// across all out-links of current page
			double dFractionalPageRank = (arrAdjPages.length > 0) ? 
			dwPageRank.get() / arrAdjPages.length : dwPageRank.get();
			
			DoubleWritable dwFractionalRank = new DoubleWritable(dFractionalPageRank);
			int i = 0;
			for (String page : arrAdjPages) {
				// Send out equal fractions of the current page
				// rank to all out-links
				context.write(
						new Text(page),
						new Node(dwFractionalRank, new Text("F"), new Text()));
			}
			
			// Pass along the node as it is to the reducer
			context.write(
					new Text(strPageName), 
					new Node(dwPageRank, new Text("R"), new Text(strAdjPages)));
		}
	}
	
}
