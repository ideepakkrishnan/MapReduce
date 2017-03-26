package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.neu.pdp.pageRank.preProcessor.SourceIdJoiner.SourceIdReducer;
import com.neu.pdp.pageRank.preProcessor.SourceIdJoiner.SourceMapper;
import com.neu.pdp.pageRank.preProcessor.adjacencyListBuilder.AdjacencyListReducer;
import com.neu.pdp.pageRank.preProcessor.adjacencyListBuilder.GroupComparator;
import com.neu.pdp.pageRank.preProcessor.adjacencyListBuilder.TokenizerMapper;
import com.neu.pdp.pageRank.preProcessor.adjacencyListBuilder.ValuePartitioner;
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
    		
    		if (args.length != 5) {
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
		        		parserJob, new Path(args[1]));
		        
		        // Add output file to store the mapping between
		        // page names and counters
		        MultipleOutputs.addNamedOutput(
		        		parserJob, 
		        		"mapping", 
		        		TextOutputFormat.class, 
		        		Text.class, Text.class);
		        
		        parserJob.waitForCompletion(true);
		        
		        long pageCount = parserJob.getCounters()
		        		.findCounter("pageCount", "pageCount")
		        		.getValue();
		        System.out.println("Number of records: " + pageCount);
		        
		        /**
		         * Mapping page names to their respective IDs
		         */
		        
		        Configuration sourceIdMapperConf = new Configuration();
		        Job sourceIdMapperJob = Job.getInstance(
		        		sourceIdMapperConf, "Page Rank Source - ID Mapper");
		        sourceIdMapperJob.setJarByClass(App.class);
		        
		        // Set the mapper
		        sourceIdMapperJob.setMapperClass(SourceMapper.class);
		        sourceIdMapperJob.setMapOutputKeyClass(Text.class);
		        sourceIdMapperJob.setMapOutputValueClass(Text.class);
		        sourceIdMapperJob.setReducerClass(SourceIdReducer.class);
		        
		        // Set the reducer's output key and value types
		        sourceIdMapperJob.setOutputKeyClass(Text.class);
		        sourceIdMapperJob.setOutputValueClass(Text.class);
		        
		        FileInputFormat.addInputPath(
		        		sourceIdMapperJob, new Path(args[1]));
		        FileInputFormat.addInputPath(
		        		sourceIdMapperJob, new Path(args[2]));
		        
		        FileOutputFormat.setOutputPath(
		        		sourceIdMapperJob, new Path(args[3]));
		        
		        sourceIdMapperJob.waitForCompletion(true);
		        
		        Configuration matrixGeneratorConfig = new Configuration();
		        matrixGeneratorConfig.setLong("pageCount", pageCount);
		        
		        Job matrixGeneratorJob = Job.getInstance(
		        		matrixGeneratorConfig, "Matrix Generator");
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
