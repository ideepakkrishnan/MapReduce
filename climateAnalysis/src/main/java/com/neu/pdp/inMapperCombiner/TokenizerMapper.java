/**
 * 
 */
package com.neu.pdp.inMapperCombiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.pdp.resources.IntTriplet;
import com.neu.pdp.resources.PairOfTriplets;
import com.neu.pdp.resources.ReadingType;

/**
 * Mapper class for climate analysis program.
 * It reads data from the input file, filters out the
 * required data and performs data aggregation before
 * forwarding it to the reducer.
 * @author ideepakkrishnan
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntTriplet> {
	
	// Class level variable used for in-mapper combining.
	// Stores records in the following format:
	// key - Station ID
	// value - ((MIN, Temperature, Count), (MAX, Temperature, Count))
	private HashMap<Text, PairOfTriplets> map = new HashMap<Text, PairOfTriplets>();
    
	/**
	 * Reads input data from the file line by line,
	 * filters out the TMIN & TMAX readings and adds
	 * them into a class level data structure which is
	 * used for in-mapper aggregation.
	 * @param key The mapper's input key
	 * @param value The mapper's input value
	 * @param context Current application context
	 * @throws IOException, InterruptedException
	 */
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// Local variables
    	Text stationId = new Text();    	
    	IntTriplet reading = null;
    	PairOfTriplets tripletPair;
    	ReadingType type = ReadingType.INVALID;
    	
    	String values[] = value.toString().split(",");
    	
        stationId.set(values[0]);
	    if (values[2].equals("TMIN")) {
		    type = ReadingType.MIN;
	    } else if (values[2].equals("TMAX")) {
		    type = ReadingType.MAX;
	    }
	  
	    if (type != ReadingType.INVALID) {
	    	if (!map.containsKey(stationId)) {
	    		map.put(stationId,
	    				new PairOfTriplets(
	    					new IntTriplet(
	    							ReadingType.MIN.getValue(), 0, 0),
	    					new IntTriplet(
	    							ReadingType.MAX.getValue(), 0, 0)));
	    	}
	    	
	    	// Get a pointer to the object storing data
	    	// for current
    		tripletPair = map.get(stationId);
    		if (type == ReadingType.MIN) {
    			reading = tripletPair.getFirst();
    		} else if (type == ReadingType.MAX) {
    			reading = tripletPair.getSecond();
    		}
    		
    		reading.setSecond(
					reading.getSecond() + Integer.parseInt(values[3]));
    		reading.setThird(
    				reading.getThird() + 1);
	    }
	    
	    type = ReadingType.INVALID;
    }
    
    /**
     * Executed after all map calls for the current task 
     * has been executed. It sends the data stored in
     * class level data structure onto the reducer for
     * processing.
     * @param context Current application context
     * @throws IOException, InterruptedException
     */
    public void cleanup(Context context) 
    		throws IOException, InterruptedException {
    	for (Map.Entry<Text, PairOfTriplets> entry: map.entrySet()) {
    		context.write(entry.getKey(), entry.getValue().getFirst());
    		context.write(entry.getKey(), entry.getValue().getSecond());
    	}
    }
    
}
