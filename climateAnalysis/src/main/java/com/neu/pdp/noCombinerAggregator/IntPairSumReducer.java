/**
 * 
 */
package com.neu.pdp.noCombinerAggregator;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.pdp.resources.IntPair;

/**
 * @author ideepakkrishnan
 *
 */
public class IntPairSumReducer extends Reducer<Text, IntPair, Text, IntPair> {
	
	private IntPair result = new IntPair();
	
	public void reduce(
			Text key, 
			Iterable<IntPair> values, 
            Context context) throws IOException, InterruptedException {
		// Local variables
		int tminSum = 0;
		int tminCount = 0;
		int tmaxSum = 0;
		int tmaxCount = 0;
		
		for (IntPair val : values) {
			if (val.getFirst() == 0) {
				tminCount += 1;
				tminSum += val.getSecond();
			} else if (val.getFirst() == 1) {
				tmaxCount += 1;
				tmaxSum += val.getSecond();
			}
		}
		
		result.set(
				(tminCount == 0) ? 0 : tminSum / tminCount, 
				(tmaxCount == 0) ? 0 : tmaxSum / tmaxCount);
		context.write(key, result);
	}

}
