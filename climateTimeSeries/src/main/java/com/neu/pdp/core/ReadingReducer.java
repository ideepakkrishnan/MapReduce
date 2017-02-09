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
 * @author ideepakkrishnan
 *
 */
public class ReadingReducer extends Reducer<KeyPair, IntTriplet, Text, Text> {
	
	private Text stationId = null;	
	private HashMap<Text, List<IntTriplet>> map;	
	
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		map = new HashMap<Text, List<IntTriplet>>();
	}
	
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
				
		// Switch the current station id in private variable that
		// manages the state
		this.stationId = key.getStationId();
		currentYear = new IntWritable(key.getYear().get());
		
		// These are records for the current station being processed.
		// Process and add them to the in-reducer combiner.
		for (IntTriplet val : values) {
			
			if (key.getYear().get() != currentYear.get()) {
				currentYear = new IntWritable(key.getYear().get());
			}
			
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
				updateMap(
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
	
	public void cleanup(Context context) 
    		throws IOException, InterruptedException {
		String result = "";
		for (Map.Entry<Text, List<IntTriplet>> entry: 
			this.map.entrySet()) {
			for (IntTriplet it: entry.getValue()) {
				 result += 
						"(" + String.valueOf(it.getFirst()) +
						", " + String.valueOf(it.getSecond()) + 
						", " + String.valueOf(it.getThird()) +
						") ";
			}
			context.write(entry.getKey(), new Text(result));
		}
	}
	
	private void updateMap(
			IntWritable currentYear, 
			int tminSum, int tminCount, 
			int tmaxSum, int tmaxCount) {
		if (!this.map.containsKey(this.stationId)) {
			this.map.put(this.stationId, new ArrayList<IntTriplet>());
		}
		
		this.map.get(stationId).add(
				new IntTriplet(
						currentYear.get(),
						(tminCount == 0) ? 0 : tminSum / tminCount, 
						(tmaxCount == 0) ? 0 : tmaxSum / tmaxCount));
	}

}
