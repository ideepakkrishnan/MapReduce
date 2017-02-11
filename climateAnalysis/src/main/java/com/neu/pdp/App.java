package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neu.pdp.resources.IntPair;
import com.neu.pdp.resources.IntTriplet;

/**
 * Course: CS6240 - Parallel Data Processing
 * -----------------------------------------
 * Homework 2: Question 1 - Climate Analysis
 * -----------------------------------------
 * The driver class for climate analysis program. It defines
 * the hadoop job which initializes and executes the MapReduce
 * program which calculates the average TMIN and TMAX
 * temperatures for each station and stores them in an output
 * file.
 */
public class App 
{
	private static final Logger logger = LogManager.getLogger(
			App.class.getName());
	
    public static void main( String[] args )
    {
    	logger.info("Inside main method");
        
    	try {
    		
    		if (args.length != 3) {
    			// Handle invalid runtime arguments
    			System.out.println("Encountered invalid arguments. Runtime arguments must be"
    					+ " in the format: <input folder> <output folder> <combiner type>");
    			System.exit(-1);
    		} else {
    			
    			// Initialize and execute the job
		    	Configuration conf = new Configuration();
		        Job job = Job.getInstance(conf, "Climate Analysis");
		        job.setJarByClass(App.class);
		        
		        // Set the mapper and reducer class based on the
		        // combiner specified within the runtime arguments
		        if (args[2].toString().equals("no-combiner")) {
		        	job.setMapperClass(com.neu.pdp.noCombiner.TokenizerMapper.class);
		        	job.setMapOutputKeyClass(Text.class);
			        job.setMapOutputValueClass(IntPair.class);
			        job.setReducerClass(com.neu.pdp.noCombiner.IntPairSumReducer.class);
		        } else if (args[2].toString().equals("with-combiner")) {
		        	job.setMapperClass(com.neu.pdp.withCombiner.TokenizerMapper.class);
		        	job.setMapOutputKeyClass(Text.class);
			        job.setMapOutputValueClass(IntTriplet.class);
			        job.setCombinerClass(com.neu.pdp.withCombiner.ReadingCombiner.class);
			        job.setReducerClass(com.neu.pdp.withCombiner.ReadingReducer.class);
		        } else if (args[2].toString().equals("in-mapper-combiner")) {
		        	job.setMapperClass(com.neu.pdp.inMapperCombiner.TokenizerMapper.class);
		        	job.setMapOutputKeyClass(Text.class);
			        job.setMapOutputValueClass(IntTriplet.class);
			        job.setReducerClass(com.neu.pdp.inMapperCombiner.ReadingReducer.class);
		        } else {
		        	System.out.println("Invalid combiner type specified. "
		        			+ "Please restart program execution.");
		        	System.exit(1);
		        }
		        
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
