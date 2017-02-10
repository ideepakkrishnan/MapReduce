/**
 * 
 */
package com.neu.pdp.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.neu.pdp.resources.IntTriplet;
import com.neu.pdp.resources.KeyPair;
import com.neu.pdp.resources.PairOfTriplets;

/**
 * Mapper class for the temperature time series program.
 * It reads temperature data from one or more files and
 * generates data in the following key-value pair format:
 * key - (station id, year)
 * value - IntTriplet which stores the reading type, sum
 *         of readings, number of readings compressed
 *         into this single record.
 * This class follows in-mapper combining approach to
 * minimize the data flow to reducers by maintaining a
 * hash map whose key and values are same as mentioned
 * above.
 * @author ideepakkrishnan
 */
public class TokenizerMapper extends Mapper<Object, Text, KeyPair, IntTriplet> {
	
	// Private class level variables
	private HashMap<KeyPair, PairOfTriplets> map;	 
	
	/**
	 * Initializes the class level variable used for
	 * in-mapper combining.
	 * @param context The current program context
	 * @throws IOException, InterruptedException
	 */
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		map = new HashMap<KeyPair, PairOfTriplets>();
	}
    
	/**
	 * The map function which reads input data from
	 * all input files and filters out the required
	 * data that needs processing. It stores the
	 * aggregated reading for each station in the
	 * class level variable used for in-mapper
	 * combining. 
	 * @param key Input key for mapper
	 * @param value Input value for mapper
	 * @param context Current application context
	 * @throws IOException, InterruptedException
	 */
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	// Local variables
    	KeyPair keyPair;
    	IntTriplet reading = null;
    	PairOfTriplets tripletPair = null;
    	int type = -1;
    	
    	// Extract the file name since the year needs to be
    	// extracted from this information
    	FileSplit fsFileSplit = (FileSplit)context.getInputSplit();
    	String strFilename = fsFileSplit.getPath().getName();
    	int currentYear = Integer.parseInt(
    			strFilename.substring(0, strFilename.length() - 4));
    	
    	// Parse the data from current file and generate
    	// key-value pairs for each station
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	
		while (itr.hasMoreTokens()) {
	    	String values[] = itr.nextToken().toString().split(",");	    	
	    	
	    	// Initialize the key for current record
	        keyPair = new KeyPair(
	        		new Text(values[0]), 
	        		new IntWritable(currentYear));
	        
	        // Internally, the program uses 0 to denote a
	        // TMIN reading and 1 to denote TMAX reading
		    if (values[2].equals("TMIN")) {
			    type = 0;
		    } else if (values[2].equals("TMAX")) {
			    type = 1;
		    }
		  
		    if (type != -1) {
		    	// We need to record the reading in a class
		    	// level variable to perform in-mapper
		    	// combining
		    	if (!this.map.containsKey(keyPair)) {
		    		this.map.put(keyPair,
		    				new PairOfTriplets(
		    					new IntTriplet(0, 0, 0),
		    					new IntTriplet(1, 0, 0)));
		    	}
		    	
		    	// tripletPair which is a PairOfTriplets
		    	// object, stores two IntTriplet objects
		    	// inside. The first one stores the data
		    	// for TMIN readings and the second one
		    	// stores the data for TMAX readings
	    		tripletPair = this.map.get(keyPair);
	    		
	    		if (type == 0) {
	    			reading = tripletPair.getFirst();
	    		} else if (type == 1) {
	    			reading = tripletPair.getSecond();
	    		}
	    		
	    		// Update the aggregated data with current
	    		// reading
	    		reading.setSecond(
						reading.getSecond() + Integer.parseInt(values[3]));
	    		reading.setThird(
	    				reading.getThird() + 1);
		    }
		    
		    // Clear out the local variables for further use
		    type = -1;
		    reading = null;
		    tripletPair = null;
		}
    }
    
    /**
     * Performs the clean up operation which in the case of
     * our mapper is to pass along all the data from aggregated
     * data structure on to the reducer.
     * @param context Current application context
     * @throws IOException, InterruptedException
     */
    public void cleanup(Context context) 
    		throws IOException, InterruptedException {
    	for (Map.Entry<KeyPair, PairOfTriplets> entry: this.map.entrySet()) {
    		context.write(entry.getKey(), entry.getValue().getFirst());
    		context.write(entry.getKey(), entry.getValue().getSecond());
    	}
    }
    
}
