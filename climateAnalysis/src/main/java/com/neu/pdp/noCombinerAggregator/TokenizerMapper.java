/**
 * 
 */
package com.neu.pdp.noCombinerAggregator;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.pdp.resources.IntPair;

/**
 * Reads each line from an input file and fetches the 
 * @author ideepakkrishnan
 *
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntPair> {
	
	private Text stationId = new Text();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\\r?\\n");
      String values[];
      int type = -1;
      while (itr.hasMoreTokens()) {
    	  // Split each line and store the required values
    	  values = itr.nextToken().split(",");
    	  stationId.set(values[0]);
    	  if (values[2] == "TMIN") {
    		  type = 0;
    	  } else if (values[2] == "TMAX") {
    		  type = 1;
    	  }
    	  
    	  if (type != -1) {
	    	  context.write(
	    			  stationId, 
	    			  new IntPair(type, Integer.parseInt(values[3])));
    	  }
      }
    }
    
}
