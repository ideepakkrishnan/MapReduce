/**
 * 
 */
package com.neu.pdp.withCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.neu.pdp.resources.IntTriplet;
import com.neu.pdp.resources.ReadingType;

/**
 * Combiner class for climate analysis program.
 * It performs aggregation of data in the mapper machine
 * before data is transferred onto reducers. This step
 * reduces the data being transferred over the network
 * and hence, we can hope for a better performance. This
 * is an optional step whose execution is not guaranteed
 * by the MapReduce job.
 * @author ideepakkrishnan
 */
public class ReadingCombiner extends Reducer<Text, IntTriplet, Text, IntTriplet> {
	
	/**
	 * Performs intermediate aggregation of data.
	 * @param key Reducer input key as Text
	 * @param values Reducer input values as list of IntTriplet-s
	 * @param context Current application context
	 */
	public void reduce(
			Text key, 
			Iterable<IntTriplet> values, 
            Context context) throws IOException, InterruptedException {
		// Local variables
		IntTriplet minTriplet = new IntTriplet();
		IntTriplet maxTriplet = new IntTriplet();
		int tminSum = 0;
		int tminCount = 0;
		int tmaxSum = 0;
		int tmaxCount = 0;
		
		// Performs data aggregation
		// Incoming records follow this format:
		// key - Station ID
		// value - (MIN/MAX, Temperature, Count)
		for (IntTriplet val : values) {
			if (val.getFirst() == ReadingType.MIN.getValue()) {
				tminSum += val.getSecond();
				tminCount += val.getThird();
			} else if (val.getFirst() == ReadingType.MAX.getValue()) {
				tmaxSum += val.getSecond();
				tmaxCount += val.getThird();
			}
		}
		
		// Pass on the aggregated data to the reducer
		// in the following format:
		// key - Station ID
		// value - (TMIN/TMAX, Temperature, Count)
		minTriplet.set(ReadingType.MIN.getValue(), tminSum, tminCount);
		context.write(key, minTriplet);
		
		maxTriplet.set(ReadingType.MAX.getValue(), tmaxSum, tmaxCount);
		context.write(key, maxTriplet);
	}
	
}
