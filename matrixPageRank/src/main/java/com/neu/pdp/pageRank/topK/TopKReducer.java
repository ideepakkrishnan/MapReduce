/**
 * 
 */
package com.neu.pdp.pageRank.topK;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.neu.pdp.pageRank.resources.SourceRankPair;

/**
 * @author ideepakkrishnan
 *
 */
public class TopKReducer extends Reducer<NullWritable, SourceRankPair, Text, DoubleWritable> {
	
	// Class level variables
	private PriorityQueue<SourceRankPair> pqNodes;
	private HashMap<Long, String> hmIdPageName;
	private String strMapFilePath;
	private List<SourceRankPair> pages;
	private FileSystem fs;
	private Path rankFilePath;
	
	public void setup(Context context) 
			throws IOException, InterruptedException {
		
		strMapFilePath = context.getConfiguration().get("mapFilePath");
		
		// Initialize a priority queue to store the top 100 pages
		// having highest page ranks.
		pqNodes = new PriorityQueue<SourceRankPair>(
				new Comparator<SourceRankPair>() {
					public int compare(SourceRankPair o1, SourceRankPair o2) {
						if (o1.getRank().get() < o2.getRank().get()) {
							return -1;
						} else if (o1.getRank().get() > o2.getRank().get()) {
							return 1;
						} else {
							return 0;
						}
					}
				});
		
		// Initialize array to store the mapping between page Id
		// and actual page name
		hmIdPageName = new HashMap<Long, String>();
		
		// Initialize file system variables to read the page name
		// - page ID mapping file into memory during clean up
		fs = FileSystem.get(context.getConfiguration());
		rankFilePath = new Path(strMapFilePath);
		
		// Array to store the final list in descending
		// order of page ranks
		pages = new ArrayList<SourceRankPair>();
		
	}
	
	public void reduce(NullWritable key, Iterable<SourceRankPair> values,
            Context context) throws IOException, InterruptedException {
		
		for (SourceRankPair node : values) {
			// Add this page into priority queue
			pqNodes.add(
					new SourceRankPair(
							new Text(""),
							new LongWritable(node.getSource().get()),
							new DoubleWritable(node.getRank().get())));
			
			if (pqNodes.size() > 100) {
				// Means that we need to remove the page
				// with lowest rank in the priority queue
				pqNodes.poll();
			}
		}
		
		// Add the pages from the priority queue into
		// the above list
		while (!pqNodes.isEmpty()) {
			SourceRankPair node = pqNodes.poll();
			pages.add(node);
			hmIdPageName.put(node.getSource().get(), null);
		}
		
	}
	
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		
		try {
			
			FSDataInputStream in = fs.open(rankFilePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));				
			
			String line = "";
			
			while ((line = br.readLine()) != null) {
				String[] arrMapRecord = line.split("\t");
				String name = arrMapRecord[0];
				Long id = Long.parseLong(arrMapRecord[1].substring(1));
				
				// Filter out only those page name - ID mappings
				// that are needed
				if (hmIdPageName.containsKey(id)) {
					hmIdPageName.put(id, name);
				}
			}
			
			// Write each page in the list into output file
			for (int i = pages.size() - 1; i >= 0 ; i--) {
				SourceRankPair node = pages.get(i);
				context.write(
						new Text(hmIdPageName.get(node.getSource().get())), 
						node.getRank());
			}
			
		} catch (FileNotFoundException f) {
			f.printStackTrace();
		}
		
	}

}
