/**
 * 
 */
package com.neu.pdp.withCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.neu.pdp.resources.IntTriplet;

/**
 * @author ideepakkrishnan
 *
 */
public class ReadingCombiner extends Reducer<Text, IntTriplet, Text, IntTriplet> {
	
	private IntTriplet minTriplet = new IntTriplet();
	private IntTriplet maxTriplet = new IntTriplet();
	
	public void reduce(
			Text key, 
			Iterable<IntTriplet> values, 
            Context context) throws IOException, InterruptedException {
		// Local variables
		int tminSum = 0;
		int tminCount = 0;
		int tmaxSum = 0;
		int tmaxCount = 0;
		
		for (IntTriplet val : values) {
			if (val.getFirst() == 0) {				
				tminSum += val.getSecond();
				tminCount += val.getThird();
			} else if (val.getFirst() == 1) {				
				tmaxSum += val.getSecond();
				tmaxCount += val.getThird();
			}
		}
		
		minTriplet.set(0, tminSum, tminCount);
		context.write(key, minTriplet);
		
		maxTriplet.set(1, tmaxSum, tmaxCount);
		context.write(key, maxTriplet);
	}
	
}
