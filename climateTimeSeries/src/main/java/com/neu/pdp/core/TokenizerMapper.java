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
 * Reads each line from an input file and fetches the 
 * @author ideepakkrishnan
 *
 */
public class TokenizerMapper extends Mapper<Object, Text, KeyPair, IntTriplet> {
	
	private KeyPair keyPair;	
	private HashMap<KeyPair, PairOfTriplets> map;	 
	
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		map = new HashMap<KeyPair, PairOfTriplets>();
	}
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {    	
    	FileSplit fsFileSplit = (FileSplit)context.getInputSplit();
    	String strFilename = fsFileSplit.getPath().getName();
    	int currentYear = Integer.parseInt(
    			strFilename.substring(0, strFilename.length() - 4));
    	
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	
		while (itr.hasMoreTokens()) {
	    	String values[] = itr.nextToken().toString().split(",");
	    	IntTriplet reading = null;
	    	PairOfTriplets tripletPair;
	    	int type = -1;
	    	
	        keyPair = new KeyPair(
	        		new Text(values[0]), 
	        		new IntWritable(currentYear));
	        
		    if (values[2].equals("TMIN")) {
			    type = 0;
		    } else if (values[2].equals("TMAX")) {
			    type = 1;
		    }
		  
		    if (type != -1) {
		    	if (!map.containsKey(keyPair)) {
		    		map.put(keyPair,
		    				new PairOfTriplets(
		    					new IntTriplet(0, 0, 0),
		    					new IntTriplet(1, 0, 0)));
		    	}
		    	
	    		tripletPair = map.get(keyPair);
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
    }
    
    public void cleanup(Context context) 
    		throws IOException, InterruptedException {
    	for (Map.Entry<KeyPair, PairOfTriplets> entry: map.entrySet()) {
    		context.write(entry.getKey(), entry.getValue().getFirst());
    		context.write(entry.getKey(), entry.getValue().getSecond());
    	}
    }
    
}
