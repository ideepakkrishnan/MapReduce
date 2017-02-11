/**
 * 
 */
package com.neu.pdp.noCombiner;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.pdp.resources.IntPair;
import com.neu.pdp.resources.ReadingType;

/**
 * Mapper class for climate analysis program.
 * It reads data from the input file, filters out the
 * required data and processes it before forwarding it
 * to the reducer for aggregation.
 * @author ideepakkrishnan
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntPair> {
    
	/**
	 * Reads input data from the file line by line and
	 * filters out the TMIN & TMAX readings before
	 * passing it on to the reducer for aggregation.
	 * @param key The mapper's input key
	 * @param value The mapper's input value
	 * @param context Current application context
	 * @throws IOException, InterruptedException
	 */
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// Local variables
		Text stationId = new Text();
		ReadingType type = ReadingType.INVALID;
		
		// Since the program is reading one line at a time,
		// we need to split it and extract the required
		// fields for further processing.
    	String values[] = value.toString().split(",");
        
        stationId.set(values[0]);
	    if (values[2].equals("TMIN")) {
		    type = ReadingType.MIN;
	    } else if (values[2].equals("TMAX")) {
		    type = ReadingType.MAX;
	    }
	  
	    if (type != ReadingType.INVALID) {
	    	// Pass on the data into reducer in following
	    	// format:
			// key - Station ID
			// value - (TMIN/TMAX, Temperature)
    	    context.write(
    		  	    stationId, 
    			    new IntPair(type.getValue(), Integer.parseInt(values[3])));
	    }
	    
	    type = ReadingType.INVALID;
    }
    
}
