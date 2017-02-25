package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.pdp.pageRank.core.NeighborRankMapper;
import com.neu.pdp.pageRank.core.NodeRankReducer;
import com.neu.pdp.pageRank.preProcessor.AdjacencyListReducer;
import com.neu.pdp.pageRank.preProcessor.GroupComparator;
import com.neu.pdp.pageRank.preProcessor.TokenizerMapper;
import com.neu.pdp.pageRank.preProcessor.ValuePartitioner;
import com.neu.pdp.resources.KeyPair;
import com.neu.pdp.resources.Node;;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	try {
    		
    		if (args.length != 2) {
    			System.out.println("Invalid argument list found. Please retry.");
    			System.exit(-1);
    		} else {
		    	Configuration parserConf = new Configuration();
		        Job parserJob = Job.getInstance(parserConf, "Page Rank pre-processor");
		        parserJob.setJarByClass(App.class);
		        
		        // Set the mapper
		        parserJob.setMapperClass(TokenizerMapper.class);
		        parserJob.setMapOutputKeyClass(KeyPair.class);
		        parserJob.setMapOutputValueClass(Text.class);
		        
		        // Set the intermediate classes
		        parserJob.setPartitionerClass(ValuePartitioner.class);
		        parserJob.setGroupingComparatorClass(GroupComparator.class);	        
		        
		        parserJob.setReducerClass(AdjacencyListReducer.class);
		        // Set the reducer's output key and value types
		        parserJob.setOutputKeyClass(Text.class);
		        parserJob.setOutputValueClass(Text.class);
		        
		        FileInputFormat.addInputPath(parserJob, new Path(args[0]));
		        FileOutputFormat.setOutputPath(
		        		parserJob, new Path(args[1] + String.valueOf(0)));
		        parserJob.waitForCompletion(true);
		        
		        long pageCount = parserJob.getCounters()
		        		.findCounter("pageCount", "pageCount")
		        		.getValue();
		        System.out.println("Number of records: " + pageCount);
		        
		        Configuration rankerConf = new Configuration();
		        rankerConf.setDouble("totalPages", pageCount);
		        rankerConf.setDouble("alpha", 0.85);
		        
		        // Execute the page rank algorithm 10 times
		        for (int i = 0; i < 2; i++) {
		        	System.out.println("Execution " + i);
			        Job rankerJob = Job.getInstance(rankerConf, "Page Rank Core");
			        rankerJob.setJarByClass(App.class);
			        
			        // Set the mapper
			        rankerJob.setMapperClass(NeighborRankMapper.class);
			        rankerJob.setMapOutputKeyClass(Text.class);
			        rankerJob.setMapOutputValueClass(Node.class);	        
			        
			        // Set the reducer
			        rankerJob.setReducerClass(NodeRankReducer.class);
			        rankerJob.setOutputKeyClass(Text.class);
			        rankerJob.setOutputValueClass(Text.class);
		        		        
		        	// Specify input folder for this iteration
	        		FileInputFormat.addInputPath(
	        				rankerJob, new Path(args[1] + String.valueOf(i)));
		        	
		        	// Specify output folder for this iteration
			        FileOutputFormat.setOutputPath(
			        		rankerJob, new Path(args[1] + String.valueOf(i + 1)));
			        
			        // Execute the job
			        rankerJob.waitForCompletion(true);
			        
		        }
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
