/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor.matrixBuilder.colVersion;

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
	private Long pageCount;
	
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		
		pageIdMap = new HashMap<String, Long>();
		pageCount = context.getConfiguration().getLong("pageCount", -1);
		
	}
	
	public void reduce(CondensedNode key, Iterable<SourceRankPair> values,
            Context context
            ) throws IOException, InterruptedException {
		
		int outlinkCount = 0;
		Long val = -1L;
		
		for (SourceRankPair p : values) {
			if (key.getRank().get() == 1) {
				// This record is a page name - ID map.
				// Store it in the hashmap for lookup.
				String temp = new String(p.getDest().toString());
				val = new Long(p.getSource().get());
				pageIdMap.put(temp, val);
			} else {
				try {
					outlinkCount += 1;
					context.write(
							new LongWritable(p.getSource().get()), 
							new Text(pageIdMap.get(p.getDest().toString()) + ":" + p.getRank().get()));
				} catch (Exception e) {
					//System.out.println("Cause: dest - " + p.getDest() + ", rank - " + p.getRank() + ", source - " + p.getSource());
					//e.printStackTrace();
				}
			}
		}
		
	}
	
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		
		
		
	}

}
