package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.pdp.pageRank.core.NeighborRankMapper;
import com.neu.pdp.pageRank.core.NodeRankReducer;
import com.neu.pdp.pageRank.preProcessor.AdjacencyListReducer;
import com.neu.pdp.pageRank.preProcessor.GroupComparator;
import com.neu.pdp.pageRank.preProcessor.TokenizerMapper;
import com.neu.pdp.pageRank.preProcessor.ValuePartitioner;
import com.neu.pdp.pageRank.topK.NodeRankMapper;
import com.neu.pdp.pageRank.topK.TopKReducer;
import com.neu.pdp.resources.CondensedNode;
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
    	int i = 0;
    	double currDelta = 0;
    	
    	try {
    		
    		if (args.length != 3) {
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
		        
		        for (; i < 10; i++) {
		        	System.out.println("Execution " + i);
		        	rankerConf.setDouble("delta", currDelta);
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
			        
			        // Update the delta value in the config
			        // for next run
			        Counters counters = rankerJob.getCounters();
			        Counter danglingCounter = counters.findCounter(DANGLING_NODES.TOTAL_PAGE_RANK);
			        currDelta = Double.longBitsToDouble(danglingCounter.getValue());
		        }
		        
		        // Execute Top-K job to find the top 100 pages
		        Configuration topKConf = new Configuration();
		        Job topKJob = Job.getInstance(topKConf, "Page Rank Top-100");
		        topKJob.setJarByClass(App.class);
		        
		        // Set the mapper
		        topKJob.setMapperClass(NodeRankMapper.class);
		        topKJob.setMapOutputKeyClass(NullWritable.class);
		        topKJob.setMapOutputValueClass(CondensedNode.class);	        
		        
		        // Set the reducer
		        topKJob.setReducerClass(TopKReducer.class);
		        topKJob.setOutputKeyClass(Text.class);
		        topKJob.setOutputValueClass(DoubleWritable.class);
		        topKJob.setNumReduceTasks(1);
	        		        
	        	// Specify input folder for this iteration
        		FileInputFormat.addInputPath(
        				topKJob, new Path(args[1] + String.valueOf(i)));
	        	
	        	// Specify output folder for this iteration
		        FileOutputFormat.setOutputPath(
		        		topKJob, new Path(args[2]));
		        
		        // Execute the job
		        topKJob.waitForCompletion(true);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
