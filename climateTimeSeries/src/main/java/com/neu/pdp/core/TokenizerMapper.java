/**
 * 
 */
package com.neu.pdp.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.pdp.resources.IntTriplet;
import com.neu.pdp.resources.PairOfTriplets;

/**
 * Reads each line from an input file and fetches the 
 * @author ideepakkrishnan
 *
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntTriplet> {
	
	private Text stationId = new Text();	
	private HashMap<Text, PairOfTriplets> map;
	
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		map = new HashMap<Text, PairOfTriplets>();
	}
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String values[] = value.toString().split(",");
    	IntTriplet reading = null;
    	PairOfTriplets tripletPair;
    	int type = -1;
    	
        stationId.set(values[0]);
	    if (values[2].equals("TMIN")) {
		    type = 0;
	    } else if (values[2].equals("TMAX")) {
		    type = 1;
	    }
	  
	    if (type != -1) {
	    	if (!map.containsKey(stationId)) {
	    		map.put(stationId,
	    				new PairOfTriplets(
	    					new IntTriplet(0, 0, 0),
	    					new IntTriplet(1, 0, 0)));
	    	}
	    	
    		tripletPair = map.get(stationId);
    		if (type == 0) {
    			reading = tripletPair.getFirst();
    		} else if (type == 1) {
    			reading = tripletPair.getSecond();
    		}
    		reading.setSecond(
					reading.getSecond() + Integer.parseInt(values[3]));
    		reading.setThird(
    				reading.getThird() + 1);
	    }
	    
	    type = -1;
    }
    
    public void cleanup(Context context) 
    		throws IOException, InterruptedException {
    	for (Map.Entry<Text, PairOfTriplets> entry: map.entrySet()) {
    		context.write(entry.getKey(), entry.getValue().getFirst());
    		context.write(entry.getKey(), entry.getValue().getSecond());
    	}
    }
    
}
