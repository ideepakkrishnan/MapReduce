package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neu.pdp.core.ReadingReducer;
import com.neu.pdp.core.TokenizerMapper;
import com.neu.pdp.resources.IntTriplet;

/**
 * Hello world!
 *
 */
public class App 
{
	private static final Logger logger = LogManager.getLogger(
			App.class.getName());
	
    public static void main( String[] args )
    {
    	logger.info("Inside main method");
        
    	try {
	    	Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Temperature Time Series");
	        job.setJarByClass(App.class);
	        
	        // Set the mapper
        	job.setMapperClass(TokenizerMapper.class);
        	job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(IntTriplet.class);
	        job.setReducerClass(ReadingReducer.class);
	        
	        // Set the reducer's output key and value types
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
	        // Set the file input and output paths
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	        // Execute the job
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
    	} catch (Exception e) {
    		logger.error(e.getMessage());
    	}
    	
        logger.info("Exiting main method");
    }
}
