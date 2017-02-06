/**
 * 
 */
package com.neu.pdp.withCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.pdp.resources.IntTriplet;

/**
 * @author ideepakkrishnan
 *
 */
public class ReadingReducer extends Reducer<Text, IntTriplet, Text, Text> {
	
	private Text result = new Text();
	
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
		
		result = new Text(
				", " + String.valueOf((tminCount == 0) ? 0 : tminSum / tminCount) + 
				", " + String.valueOf((tmaxCount == 0) ? 0 : tmaxSum / tmaxCount));
		
		context.write(key, result);
	}

}
