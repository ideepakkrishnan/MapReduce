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
 * @author ideepakkrishnan
 *
 */
public class NeighborRankMapper extends Mapper<Object, Text, Text, Node> {

	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		String strLine = value.toString();
		String strLineSplit[] = strLine.split("\t");
		String strPageName = strLineSplit[0];
		String strAdjPages = "";
		String arrAdjPages[] = new String[0];
		DoubleWritable dwPageRank = new DoubleWritable(-1);		
		
		if (strLineSplit.length > 1 && strLineSplit[1].indexOf(':') >= 0) {
			String strValSplit[] = strLineSplit[1].split(":");
			dwPageRank = new DoubleWritable(Double.parseDouble(strValSplit[0]));
			if (strValSplit != null && strValSplit.length > 0) {
				strAdjPages = strValSplit[1];
				arrAdjPages = strValSplit[1].split(",");
			}
		} else {
			dwPageRank = new DoubleWritable(0.23);
			if (strLineSplit.length > 1) {
				strAdjPages = strLineSplit[1];
				arrAdjPages = strLineSplit[1].split(",");
			}
		}
		
		double dFractionalPageRank = (arrAdjPages.length > 0) ? 
		dwPageRank.get() / arrAdjPages.length : dwPageRank.get();
		DoubleWritable dwFractionalRank = new DoubleWritable(dFractionalPageRank);
		int i = 0;
		for (String page : arrAdjPages) {
			// Send out equal fractions of the current page
			// rank to all out-links
			context.write(
					new Text(page),
					new Node(dwFractionalRank, new Text()));
		}
		
		// Pass along the node as it is to the reducer
		context.write(
				new Text(strPageName), 
				new Node(dwPageRank, new Text(strAdjPages)));
	}
	
}
