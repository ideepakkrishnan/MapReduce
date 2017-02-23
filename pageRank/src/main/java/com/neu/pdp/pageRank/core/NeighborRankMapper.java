/**
 * 
 */
package com.neu.pdp.pageRank.core;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.neu.pdp.resources.Node;
import com.neu.pdp.resources.TextArrayWritable;

/**
 * @author ideepakkrishnan
 *
 */
public class NeighborRankMapper extends Mapper<Text, Text, Text, Node> {

	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		String strLine = value.toString();
		String strLineSplit[] = strLine.split("\t");
		String strPageName = strLineSplit[0];
		String strAdjPages[];
		DoubleWritable iPageRank = new DoubleWritable(-1);		
		
		if (strLineSplit[1].indexOf(':') >= 0) {
			String strValSplit[] = strLineSplit[1].split(":");
			iPageRank = new DoubleWritable(Double.parseDouble(strValSplit[0]));
			strAdjPages = strValSplit[1].split(",");
		} else {
			iPageRank = new DoubleWritable(0.75);
			strAdjPages = strLineSplit[1].split(",");
		}		
		
		Text[] arrAdjPages = new Text[strAdjPages.length];
		int i = 0;
		for (String page : strAdjPages) {
			arrAdjPages[i] = new Text(page);
		}
		
		context.write(
				new Text(strPageName), 
				new Node(new DoubleWritable(-1), new TextArrayWritable(arrAdjPages)));
	}
	
}
