/**
 * 
 */
package com.neu.pdp.pageRank.core.colVersion;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.neu.pdp.pageRank.resources.SourceRankPair;

/**
 * @author ideepakkrishnan
 *
 */
public class RowColumnReducer extends Reducer<LongWritable, SourceRankPair, LongWritable, DoubleWritable> {
	
	private HashMap<Long, Double> hmPrevRanks;
	private double dAlpha;
	private double dDelta;
	private Long lPageCount;
	private String strRankFilePath;
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		
		if (context.getCacheFiles() != null &&
				context.getCacheFiles().length > 0) {
			dAlpha = context.getConfiguration().getDouble("alpha", 0.85);
			dDelta = context.getConfiguration().getDouble("delta", 0);
			lPageCount = context.getConfiguration().getLong("pageCount", -1);
			strRankFilePath = context.getConfiguration().get("rankFilePath", "");
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path rankFilePath = new Path(strRankFilePath);
			
			try {
				FSDataInputStream in = fs.open(rankFilePath);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
			
				hmPrevRanks = new HashMap<Long, Double>();
				
				String line = "";
				
				while ((line = br.readLine()) != null) {
					String[] arrRanks = line.split("\t");
					hmPrevRanks.put(
							Long.parseLong(arrRanks[0]), 
							Double.parseDouble(arrRanks[1]));
				}
				
			} catch (FileNotFoundException f) {
				f.printStackTrace();
			}
		}
		
	}
	
	public void reduce(LongWritable key, Iterable<SourceRankPair> values,
            Context context
            ) throws IOException, InterruptedException {
		
		double dIncomingContributions = 0;
		double newRank = 0;
		double rowIdRank = hmPrevRanks.get(key.get());
		
		for (SourceRankPair p : values) {
			try {
				dIncomingContributions += (p.getRank().get() * rowIdRank);
			} catch (Exception e) {
				// Do nothing
			}
		}
		
		// Calculate the final row value using the following
		// formula: (alpha / page-count) + (1 - alpha) * incoming-contributions
		newRank = (dAlpha / lPageCount) + 
				((1 - dAlpha) * ((dDelta / lPageCount) + dIncomingContributions));
		
		hmPrevRanks.remove(key.get());
		
		context.write(key, new DoubleWritable(newRank));
	}
	
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		// Safety mechanism for pages with no in-links. Being added
		// as a temporary measure since the dangling nodes are yet
		// to be handled.
		// TODO Remove this once dangling nodes are handled
		for (Map.Entry<Long, Double> entry : hmPrevRanks.entrySet()) {
			context.write(
					new LongWritable(entry.getKey()), 
					new DoubleWritable((dAlpha / lPageCount)));
		}
	}

}
