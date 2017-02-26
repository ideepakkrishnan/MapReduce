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

import com.neu.pdp.resources.KeyPair;

/**
 * @author ideepakkrishnan
 *
 */
public class AdjacencyListReducer extends Reducer<KeyPair, Text, Text, Text> {
	
	// Class level private variables
	private HashSet<Text> pageNames;	
	
	/**
	 * Initialize the class level variables for in-
	 * reducer combining
	 * @param Current application context
	 */
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		pageNames = new HashSet<Text>();
	}
	
	/**
	 * 
	 */
	public void reduce(KeyPair key, Iterable<Text> values,
            Context context
            ) throws IOException, InterruptedException {
		
		// Local variables
		String strAdjacencyList = "";
		
		if (key.getFirst().toString().equals("COUNT") && 
				!key.getSecond().toString().equals("ADJ")) {
			for (Text t : values) {
				pageNames.add(t);
			}
		} else {		
			for (Text t : values) {
				if (t.getLength() > 0) {
					strAdjacencyList += t.toString();
				}
			}
			
			context.write(key.getFirst(), new Text(strAdjacencyList));
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		int count = 0;
		
		for (Text t : pageNames) {
			count++;
		}
		
		context.getCounter("pageCount", "pageCount").setValue(count);
	}

}
