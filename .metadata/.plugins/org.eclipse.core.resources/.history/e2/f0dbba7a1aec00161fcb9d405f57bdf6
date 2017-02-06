/**
 * 
 */
package com.neu.pdp.withCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.pdp.resources.IntTriplet;

/**
 * Reads each line from an input file and fetches the 
 * @author ideepakkrishnan
 *
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntTriplet> {
	
	private Text stationId = new Text();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String values[] = value.toString().split(",");
    	int type = -1;
        stationId.set(values[0]);
	    if (values[2].equals("TMIN")) {
		    type = 0;
	    } else if (values[2].equals("TMAX")) {
		    type = 1;
	    }
	  
	    if (type != -1) {
    	    context.write(
    		  	    stationId, 
    			    new IntTriplet(type, Integer.parseInt(values[3]), 1));
	    }
	    
	    type = -1;
    }
    
}
