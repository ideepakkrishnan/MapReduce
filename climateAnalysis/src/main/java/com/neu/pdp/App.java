package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neu.pdp.noCombinerAggregator.IntPairSumReducer;
import com.neu.pdp.noCombinerAggregator.TokenizerMapper;
import com.neu.pdp.resources.IntPair;

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
	        Job job = Job.getInstance(conf, "Climate Analysis");
	        job.setJarByClass(App.class);
	        job.setMapperClass(TokenizerMapper.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(IntPair.class);
	        job.setCombinerClass(IntPairSumReducer.class);
	        job.setReducerClass(IntPairSumReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
    	} catch (Exception e) {
    		logger.error(e.getMessage());
    	}
    	
        logger.info("Exiting main method");
    }
}
