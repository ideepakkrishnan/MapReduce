/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.core;

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
	
	HashMap<Long, Double> hmPrevRanks; 
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		
		if (context.getCacheFiles() != null &&
				context.getCacheFiles().length > 0) {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path rankFilePath = new Path("/home/ideepakkrishnan/Documents/pageRank/mergedRank/ranks");
			System.out.println("Reduce cache path: " + rankFilePath.toString());
			
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
		
		double newRank = 0;
		
		for (SourceRankPair p : values) {
			newRank += (p.getRank().get() * hmPrevRanks.get(p.getSource().get()));
		}
		
		hmPrevRanks.remove(key.get());
		
		context.write(key, new DoubleWritable(newRank));
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<Long, Double> entry : hmPrevRanks.entrySet()) {
			context.write(
					new LongWritable(entry.getKey()), 
					new DoubleWritable(entry.getValue()));
		}
	}

}
