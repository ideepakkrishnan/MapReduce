/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.matrixBuilder;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.neu.pdp.pageRank.resources.CondensedNode;
import com.neu.pdp.pageRank.resources.SourceRankPair;

/**
 * @author ideepakkrishnan
 *
 */
public class PageNameIdReducer extends Reducer<CondensedNode, SourceRankPair, LongWritable, Text> {
	
	private HashMap<String, Long> pageIdMap;
	
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		
		pageIdMap = new HashMap<String, Long>();
		
	}
	
	public void reduce(CondensedNode key, Iterable<SourceRankPair> values,
            Context context
            ) throws IOException, InterruptedException {
		
		for (SourceRankPair p : values) {
			if (key.getRank().get() == 1) {
				// This record is a page name - ID map.
				// Store it in the hashmap for lookup.
				String temp = new String(p.getDest().toString());
				Long val = new Long(p.getSource().get());
				pageIdMap.put(temp, val);
			} else {
				try {
				context.write(
						new LongWritable(pageIdMap.get(p.getDest().toString())), 
						new Text(p.getSource().get() + ":" + p.getRank().get()));
				} catch (Exception e) {
					System.out.println("Cause: dest - " + p.getDest() + ", rank - " + p.getRank() + ", source - " + p.getSource());
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		
		
		
	}

}