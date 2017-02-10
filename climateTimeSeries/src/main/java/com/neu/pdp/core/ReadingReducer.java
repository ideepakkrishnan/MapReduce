/**
 * 
 */
package com.neu.pdp.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.neu.pdp.resources.IntTriplet;
import com.neu.pdp.resources.KeyPair;

/**
 * Reducer class for temperature time series program.   
 * Each reducer call gets the complete data for the
 * station it is processing. This class follows the
 * in-reducer combining approach so that the calculated
 * yearly average readings are grouped by station id.
 * In order to do this, the class maintains a class
 * level hash map variable whose key is the station id
 * and value is a list of IntTriplet-s where each 
 * triplet represents the average min and max reading
 * for a year. This list is sorted upon its year in
 * increasing order.
 * @author ideepakkrishnan
 */
public class ReadingReducer extends Reducer<KeyPair, IntTriplet, Text, Text> {
	
	// Class level private variables
	private HashMap<Text, List<IntTriplet>> map;	
	
	/**
	 * Initialize the class level variables for in-
	 * reducer combining
	 * @param Current application context
	 */
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		map = new HashMap<Text, List<IntTriplet>>();
	}
	
	/**
	 * The function executed in each reducer call to
	 * perform the required calculation.
	 * @param key A KeyPair object (Same as mapper 
	 * output key)
	 * @param values A list of IntTriplet objects
	 * (aggregation of mapper output values based
	 * on logic specified in GroupComparator)
	 * @param context Current application context
	 * @throws IOException, InterruptedException
	 */
	public void reduce(
			KeyPair key, 
			Iterable<IntTriplet> values, 
            Context context) throws IOException, InterruptedException {
		// Local variables
		IntWritable currentYear = null;
		int tminSum = 0;
		int tminCount = 0;
		int tmaxSum = 0;
		int tmaxCount = 0;
		boolean avgTminWritten = false;
		boolean avgTmaxWritten = false;
		
		// Extracting the year into a new IntWritable object to
		// prevent memory leaks down the lane
		currentYear = new IntWritable(key.getYear().get());
		
		// These are records for the current station being processed.
		// Process and add them to the in-reducer combiner.
		for (IntTriplet val : values) {
			
			if (key.getYear().get() != currentYear.get()) {
				currentYear = new IntWritable(key.getYear().get());
			}
			
			// Aggregate the readings and record counts to calculate
			// the mean
			if (val.getFirst() == 0) {
				tminSum += val.getSecond();
				tminCount += val.getThird();
				avgTminWritten = true;
			} else if (val.getFirst() == 1) {				
				tmaxSum += val.getSecond();
				tmaxCount += val.getThird();
				avgTmaxWritten = true;
			}
			
			if (avgTminWritten && avgTmaxWritten) {
				// If we get inside this if statement, it means that
				// the data for a particular year has been processed.
				// We need to write this data into the class level
				// variable so that it can be written to output file
				// later.
				updateMap(
						key.getStationId(),
						currentYear, 
						tminSum, tminCount, 
						tmaxSum, tmaxCount);
				
				// Reset all counters				
				tminSum = 0;
				tminCount = 0;
				tmaxSum = 0;
				tmaxCount = 0;
				avgTminWritten = false;
				avgTmaxWritten = false;
			}
		}
		
	}
	
	/**
	 * Performs the final task of writing the calculated
	 * values stored in the class level variable into an
	 * output file.
	 * @param context Current application context
	 * @throws IOException, InterruptedException
	 */
	public void cleanup(Context context) 
    		throws IOException, InterruptedException {
		// Local variables
		String result = "";
		
		// Iterate through the class level hash map and
		// write each record into an output file
		for (Map.Entry<Text, List<IntTriplet>> entry: 
			this.map.entrySet()) {
			result = "[ ";
			for (IntTriplet it: entry.getValue()) {
				 result += 
						"(" + String.valueOf(it.getFirst()) +
						", " + String.valueOf(it.getSecond()) + 
						", " + String.valueOf(it.getThird()) +
						"), ";
			}
			result += "]";
			context.write(entry.getKey(), new Text(result));
			
			result = "";
		}
	}
	
	/**
	 * Calculates the average min and max temperatures
	 * from the data passed in as arguments and writes
	 * it into the class level hash map.
	 * @param stationId Station ID as Text
	 * @param currentYear Year as IntWritable
	 * @param tminSum Sum of all TMINs as integer
	 * @param tminCount Count of TMIN records as integer
	 * @param tmaxSum Sum of all TMAXs as integer
	 * @param tmaxCount Count of TMAX records as integer
	 */
	private void updateMap(
			Text stationId,
			IntWritable currentYear, 
			int tminSum, int tminCount, 
			int tmaxSum, int tmaxCount) {
		if (!this.map.containsKey(stationId)) {
			this.map.put(
					new Text(stationId.getBytes()), 
					new ArrayList<IntTriplet>());
		}
		
		// Calculate and append the average values into
		// hash map
		this.map.get(stationId).add(
				new IntTriplet(
						currentYear.get(),
						(tminCount == 0) ? 0 : tminSum / tminCount, 
						(tmaxCount == 0) ? 0 : tmaxSum / tmaxCount));
	}

}
