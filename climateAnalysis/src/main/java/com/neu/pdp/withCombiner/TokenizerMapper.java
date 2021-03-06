/**
 * 
 */
package com.neu.pdp.withCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.pdp.resources.IntTriplet;
import com.neu.pdp.resources.ReadingType;

/**
 * Mapper class for climate analysis program.
 * It reads data from the input file, filters out the
 * required data and processes it before forwarding it
 * to the combiner/reducer for aggregation. 
 * @author ideepakkrishnan
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntTriplet> {
	
	/**
	 * Reads input data from the file line by line and
	 * filters out the TMIN & TMAX readings before
	 * passing it on to the combiner/reducer.
	 * @param key The mapper's input key
	 * @param value The mapper's input value
	 * @param context Current application context
	 */
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// Local variables
		Text stationId = new Text();
    	ReadingType type = ReadingType.INVALID;

    	// Process input string and fetch required values
    	String values[] = value.toString().split(",");
    	
        stationId.set(values[0]);
	    if (values[2].equals("TMIN")) {
		    type = ReadingType.MIN;
	    } else if (values[2].equals("TMAX")) {
		    type = ReadingType.MAX;
	    }
	  
	    if (type != ReadingType.INVALID) {
	    	// Pass on the record to combiner/reducer.
	    	// The value has the following format:
	    	// (<Reading Type>, <Temperature>, <Count>)
    	    context.write(
    		  	    stationId, 
    			    new IntTriplet(
    			    		type.getValue(), 
    			    		Integer.parseInt(values[3]), 1));
	    }
	    
	    type = ReadingType.INVALID;
    }
    
}
