/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.adjacencyListBuilder;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.neu.pdp.pageRank.resources.KeyPair;

/**
 * Reducer class for pre-processor which generates
 * the actual adjacency lists and the total number
 * of pages in the data-set for the next phase. 
 * @author ideepakkrishnan
 */
public class AdjacencyListReducer extends Reducer<KeyPair, Text, Text, Text> {
	
	// Class level private variables
	private HashSet<String> pageNames;
	private MultipleOutputs<Text, Text> mos;
	private String outputFolder;
	
	/**
	 * Initialize the class level variables for in-
	 * reducer combining
	 * @param Current application context
	 */
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		pageNames = new HashSet<String>();
		mos = new MultipleOutputs(context);
		outputFolder = context.getConfiguration().get("mappingOutput");
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
			// Dummy nodes being passed in to find the
			// number of pages
			for (Text t : values) {
				pageNames.add(t.toString());
			}
		} else {
			// Actual pages and their adjacency lists
			// which needs to be passed on to the next
			// phase.
			for (Text t : values) {
				if (t.getLength() > 0) {
					strAdjacencyList += t.toString();
				}
			}
			
			context.write(key.getFirst(), new Text(strAdjacencyList));
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		// Calculate the total number of pages and 
		// save it in a global counter so that it
		// can be used in the next phase.
		int count = 0;
		
		if (outputFolder != null) {
			for (String s : pageNames) {
				count++;			
				mos.write(new Text(s), new Text("/" + count), outputFolder + "/map");
			}
			
			mos.close();
		} else {
			throw new IOException("Mapping output folder not found!");
		}
		
		context.getCounter("pageCount", "pageCount").setValue(count);
	}

}
