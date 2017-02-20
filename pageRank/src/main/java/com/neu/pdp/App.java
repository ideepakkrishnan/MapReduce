package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.pdp.pageRank.preProcessor.AdjacencyListReducer;
import com.neu.pdp.pageRank.preProcessor.TokenizerMapper;;

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
		        Job job = Job.getInstance(conf, "Page Rank");
		        job.setJarByClass(App.class);
		        job.setMapperClass(TokenizerMapper.class);
		        job.setCombinerClass(AdjacencyListReducer.class);
		        job.setReducerClass(AdjacencyListReducer.class);
		        job.setOutputKeyClass(Text.class);
		        job.setOutputValueClass(Text.class);
		        FileInputFormat.addInputPath(job, new Path(args[0]));
		        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		        System.exit(job.waitForCompletion(true) ? 0 : 1);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
