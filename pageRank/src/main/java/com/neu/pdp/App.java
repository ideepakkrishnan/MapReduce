package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.pdp.pageRank.preProcessor.AdjacencyListReducer;
import com.neu.pdp.pageRank.preProcessor.GroupComparator;
import com.neu.pdp.pageRank.preProcessor.TokenizerMapper;
import com.neu.pdp.pageRank.preProcessor.ValuePartitioner;
import com.neu.pdp.resources.KeyPair;;

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
		    	Configuration conf = new Configuration();
		        Job job = Job.getInstance(conf, "Page Rank pre-processor");
		        job.setJarByClass(App.class);
		        
		        // Set the mapper
	        	job.setMapperClass(TokenizerMapper.class);
	        	job.setMapOutputKeyClass(KeyPair.class);
		        job.setMapOutputValueClass(Text.class);
		        
		        // Set the intermediate classes
		        job.setPartitionerClass(ValuePartitioner.class);
		        job.setGroupingComparatorClass(GroupComparator.class);	        
		        
		        job.setReducerClass(AdjacencyListReducer.class);
		        // Set the reducer's output key and value types
		        job.setOutputKeyClass(Text.class);
		        job.setOutputValueClass(Text.class);
		        
		        FileInputFormat.addInputPath(job, new Path(args[0]));
		        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		        job.waitForCompletion(true);
		        
		        long pageCount = job.getCounters()
		        		.findCounter("pageCount", "pageCount")
		        		.getValue();
		        System.out.println("Number of records: " + pageCount);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
