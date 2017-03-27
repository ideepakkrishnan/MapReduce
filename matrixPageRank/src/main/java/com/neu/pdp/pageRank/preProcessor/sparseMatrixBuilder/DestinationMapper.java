/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.sparseMatrixBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @author ideepakkrishnan
 *
 */
public class DestinationMapper extends Mapper<Object, Text, Text, Text> {
	
	// Each record represents a row in the matrix
	// Key - Row ID, Value - All column IDs which are
	// not null and their values in the format -
	// <j>:<1/C(j)> where C(j) represents the
	// number of outlinks from source j.
	private HashMap<String, String> destSourceMap;
	
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		
		// Initialize the class level variables which
		// are to be used for in-mapper combining.
		destSourceMap = new HashMap<String, String>();
		
	}
	
	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		
		String dest = "";
		String val = "";
		
		String strLine = value.toString();
		
		if (strLine != null && strLine.length() > 0) {
			// Split the record and fetch page name
			String strLineSplit[] = strLine.split("\t");
			
			if (strLineSplit.length == 2) {
				dest = strLineSplit[0];
				val = strLineSplit[1];
				
				// Add value to the hashmap
				String record = destSourceMap.get(dest);
				if (record != null) {
					// Means that we already have a record
					// for this row. So we just need to
					// append the value to it.
					destSourceMap.put(dest, record + "," + val);					
				} else {
					// Means that this is the first entry for
					// this row
					destSourceMap.put(dest, val);
				}
			} else {
				// Invalid record. Do nothing.
			}
			
		}
		
	}
	
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		
		// Forward all data stored in the hashmap to
		// reducer
		for (Map.Entry<String, String> entry: destSourceMap.entrySet()) {
			context.write(
					new Text(entry.getKey()), 
					new Text(entry.getValue()));
		}
		
	}

}
