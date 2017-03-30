/**
 * 
 */
package com.neu.pdp.pageRank.core.danglerHandler;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.neu.pdp.DANGLING_NODES;
import com.neu.pdp.pageRank.resources.CondensedNode;

/**
 * @author ideepakkrishnan
 *
 */
public class DanglerRankReducer extends Reducer<CondensedNode, CondensedNode, NullWritable, NullWritable> {
	
	private HashMap<Long, Double> pageIdRankMap;
	
	public void setup(Context context) 
    		throws IOException, InterruptedException {
		pageIdRankMap = new HashMap<Long, Double>();
	}
	
	public void reduce(CondensedNode key, Iterable<CondensedNode> values,
            Context context
            ) throws IOException, InterruptedException {
		
		for (CondensedNode cn : values) {
			if (cn.getRank().get() == Double.NEGATIVE_INFINITY) {
				// We are dealing with the dangling node list.
				// Update the global counter with the rank of
				// this node
				double val = Double.longBitsToDouble(context.getCounter(
						DANGLING_NODES.TOTAL_PAGE_RANK).getValue());
				
				val += pageIdRankMap.get(
						Long.parseLong(cn.getName().toString()));
				
				context.getCounter(
						DANGLING_NODES.TOTAL_PAGE_RANK).setValue(
								Double.doubleToLongBits(val));
			} else {
				// We are dealing with a rank record. Save
				// it in the hashmap for further usage.
				pageIdRankMap.put(
						Long.parseLong(cn.getName().toString()), 
						cn.getRank().get());
			}
		}
		
	}

}
