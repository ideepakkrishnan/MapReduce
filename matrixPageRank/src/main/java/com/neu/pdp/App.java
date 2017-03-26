package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.pdp.pageRank.preProcessor.AdjacencyListReducer;
import com.neu.pdp.pageRank.preProcessor.GroupComparator;
import com.neu.pdp.pageRank.preProcessor.TokenizerMapper;
import com.neu.pdp.pageRank.preProcessor.ValuePartitioner;
import com.neu.pdp.pageRank.resources.KeyPair;

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
    			
    			/**
    			 * Pre-processor (To generate the adjacency lists)
    			 */
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
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
