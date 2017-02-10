package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neu.pdp.core.GroupComparator;
import com.neu.pdp.core.KeyComparator;
import com.neu.pdp.core.ReadingReducer;
import com.neu.pdp.core.StationIdPartitioner;
import com.neu.pdp.core.TokenizerMapper;
import com.neu.pdp.resources.IntTriplet;
import com.neu.pdp.resources.KeyPair;

/**
 * Course: CS6240 - Parallel Data Processing
 * ------------------------------------------------
 * Homework 2: Question 2 - Temperature Time Series
 * ------------------------------------------------
 * Driver class for temperature time series application. This
 * class defines the MapReduce job which calculates the yearly
 * average min and max temperatures for each station for a
 * period of 10 years. Yearly data is expected in a CSV file
 * whose name is the year (eg. Yearly data for the year 1809 is
 * expected in 1809.csv).
 * @author Deepak Krishnan
 * @email krishnan.d[at]husky[dot]neu[dot]edu
 */
public class App 
{
	private static final Logger logger = LogManager.getLogger(
			App.class.getName());
	
    public static void main( String[] args )
    {
    	logger.info("Inside main method");
        
    	try {
    		
    		// Validate arguments:
    		// The program expects two arguments:
    		//  1. The folder path where input files are stored
    		//  2. The folder path where output files can be stored
    		// (Note: Output path must point to a non-existent folder)
    		if (args.length != 2) {
    			System.out.println("Invalid arguments found! Please pass"
    					+ " the arguments in following format: "
    					+ " <input folder> <output folder>");
    			System.exit(-1);
    		} else {
    			
	    		// Create a job to execute the MapReduce program
		    	Configuration conf = new Configuration();
		        Job job = Job.getInstance(conf, "Temperature Time Series");
		        job.setJarByClass(App.class);
		        
		        // Set the mapper
	        	job.setMapperClass(TokenizerMapper.class);
	        	job.setMapOutputKeyClass(KeyPair.class);
		        job.setMapOutputValueClass(IntTriplet.class);
		        
		        // Set the intermediate classes
		        job.setPartitionerClass(StationIdPartitioner.class);
		        job.setSortComparatorClass(KeyComparator.class);
		        job.setGroupingComparatorClass(GroupComparator.class);	        
		        
		        job.setReducerClass(ReadingReducer.class);	        
		        // Set the reducer's output key and value types
		        job.setOutputKeyClass(Text.class);
		        job.setOutputValueClass(Text.class);
		        
		        // Set the file input and output paths
		        FileInputFormat.addInputPath(job, new Path(args[0]));
		        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		        
		        // Execute the job
		        System.exit(job.waitForCompletion(true) ? 0 : 1);
		        
    		}
    	} catch (Exception e) {
    		logger.error(e.getMessage());
    	}
    	
        logger.info("Exiting main method");
    }
}
