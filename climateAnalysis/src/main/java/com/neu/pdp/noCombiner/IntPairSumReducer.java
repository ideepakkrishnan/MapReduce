/**
 * 
 */
package com.neu.pdp.noCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.pdp.resources.IntPair;
import com.neu.pdp.resources.ReadingType;

/**
 * Reducer class for climate analysis program. Since
 * the program uses the default hash partitioner on
 * the key to forward data into specific reducers,
 * all data pertaining to a station ID is accepted
 * by a specific reducer.
 * @author ideepakkrishnan
 */
public class IntPairSumReducer extends Reducer<Text, IntPair, Text, Text> {
	
	/**
	 * Performs the actual aggregation of data during
	 * a reduce call. All data pertaining to a specific
	 * station ID is accepted by a reduce call.
	 * @param key Reducer input key as Text
	 * @param values Reducer input values as list of IntPair-s
	 * @param context Current application context
	 */
	public void reduce(
			Text key, 
			Iterable<IntPair> values, 
            Context context) throws IOException, InterruptedException {
		// Local variables
		String txt = "";
		Text result = new Text();
		int tminSum = 0;
		int tminCount = 0;
		int tmaxSum = 0;
		int tmaxCount = 0;
		
		// Perform local aggregation using local variables.
		// Incoming records are follow this format:
		// key - Station ID
		// value - (TMIN/TMAX, Temperature)
		for (IntPair val : values) {
			if (val.getFirst() == ReadingType.MIN.getValue()) {
				tminCount += 1;
				tminSum += val.getSecond();
			} else if (val.getFirst() == ReadingType.MAX.getValue()) {
				tmaxCount += 1;
				tmaxSum += val.getSecond();
			}
		}
		
		// Create the result string to be written into output file
		if (tminCount == 0) {
			txt += "No Readings";
		} else {
			txt += String.valueOf(tminSum / tminCount);
		}
		
		txt += ", ";
		
		if (tmaxCount == 0) {
			txt += "No Readings";
		} else {
			txt += String.valueOf(tmaxSum / tmaxCount);
		}
		result = new Text(txt);
		
		// Output the aggregated value into output file
		context.write(key, result);
	}

}
