/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author ideepakkrishnan
 *
 */
public class AdjacencyListReducer extends Reducer<Text, Text, Text, Text> {
	
	// Class level private variables
	private HashMap<Text, HashSet<String>> map;
	
	/**
	 * Initialize the class level variables for in-
	 * reducer combining
	 * @param Current application context
	 */
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		map = new HashMap<Text, HashSet<String>>();
	}
	
	/**
	 * 
	 */
	public void reduce(Text key, Iterable<Text> values,
            Context context
            ) throws IOException, InterruptedException {
		
		// Local variables
		String strAdjacencyList = "";
		
		for (Text t : values) {
			if (t.getLength() > 0) {
				strAdjacencyList += t.toString();
			}
		}
		
		context.write(key, new Text(strAdjacencyList));
	}

}
