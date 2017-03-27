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
import com.neu.pdp.pageRank.preProcessor.matrixBuilder.FirstLetterGoupingComparator;
import com.neu.pdp.pageRank.preProcessor.matrixBuilder.FirstLetterPartitioner;
import com.neu.pdp.pageRank.preProcessor.matrixBuilder.PageNameIdReducer;
import com.neu.pdp.pageRank.preProcessor.matrixBuilder.PageNameMapper;
import com.neu.pdp.pageRank.preProcessor.sparseMatrixBuilder.DestinationMapper;
import com.neu.pdp.pageRank.preProcessor.sparseMatrixBuilder.DestinationReducer;
import com.neu.pdp.pageRank.resources.CondensedNode;
import com.neu.pdp.pageRank.resources.KeyPair;
import com.neu.pdp.pageRank.resources.SourceRankPair;

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
    		
    		if (args.length != 8) {
    			System.out.println("Invalid argument list found. Please retry.");
    			System.exit(-1);
    		} else {
    			
    			String wikiDataPath = args[0];
    			String adjListPath = args[1];
    			String pageIdMapPath = args[2];
    			String sourceIdReplacedPath = args[3];
    			String outlinkIdReplacedPath = args[4];
    			String defaultRankPath = args[5];
    			String sparseMatrixPath = args[6];
    			String topKPath = args[7];
    			
    			/**
    			 * Pre-processor (To generate the adjacency lists)
    			 */
		    	Configuration parserConf = new Configuration();
		    	parserConf.set("mappingOutput", pageIdMapPath);
		    	
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
		        
		        FileInputFormat.addInputPath(parserJob, new Path(wikiDataPath));
		        FileOutputFormat.setOutputPath(
		        		parserJob, new Path(adjListPath));
		        
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
		        sourceIdMapperConf.setLong("totalPageCount", pageCount);
		        sourceIdMapperConf.set("defaultRankPath", defaultRankPath);
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
		        		sourceIdMapperJob, new Path(adjListPath));
		        FileInputFormat.addInputPath(
		        		sourceIdMapperJob, new Path(pageIdMapPath));
		        
		        FileOutputFormat.setOutputPath(
		        		sourceIdMapperJob, new Path(sourceIdReplacedPath));
		        
		        // Add output file to store the mapping between
		        // page names and counters
		        MultipleOutputs.addNamedOutput(
		        		sourceIdMapperJob, 
		        		"ranks0", 
		        		TextOutputFormat.class, 
		        		Text.class, Text.class);
		        
		        sourceIdMapperJob.waitForCompletion(true);
		        
		        Configuration matrixGeneratorConfig = new Configuration();
		        matrixGeneratorConfig.setLong("pageCount", pageCount);
		        
		        Job matrixGeneratorJob = Job.getInstance(
		        		matrixGeneratorConfig, "Matrix Generator");
		        
		        // Set the mapper
		        matrixGeneratorJob.setMapperClass(PageNameMapper.class);
		        matrixGeneratorJob.setMapOutputKeyClass(CondensedNode.class);
		        matrixGeneratorJob.setMapOutputValueClass(SourceRankPair.class);
		        
		        // Set the intermediate classes
		        matrixGeneratorJob.setPartitionerClass(FirstLetterPartitioner.class);
		        matrixGeneratorJob.setGroupingComparatorClass(FirstLetterGoupingComparator.class);	        
		        
		        matrixGeneratorJob.setReducerClass(PageNameIdReducer.class);
		        // Set the reducer's output key and value types
		        matrixGeneratorJob.setOutputKeyClass(LongWritable.class);
		        matrixGeneratorJob.setOutputValueClass(Text.class);
		        
		        FileInputFormat.addInputPath(matrixGeneratorJob, new Path(pageIdMapPath));
		        FileInputFormat.addInputPath(matrixGeneratorJob, new Path(sourceIdReplacedPath));
		        FileOutputFormat.setOutputPath(
		        		matrixGeneratorJob, new Path(outlinkIdReplacedPath));
		        
		        matrixGeneratorJob.waitForCompletion(true);
		        
		        Configuration matrixBuilderConf = new Configuration();
		        Job matrixBuilderJob = Job.getInstance(
		        		sourceIdMapperConf, "Sparse Matrix Builder");
		        matrixBuilderJob.setJarByClass(App.class);
		        
		        // Set the mapper
		        matrixBuilderJob.setMapperClass(DestinationMapper.class);
		        matrixBuilderJob.setMapOutputKeyClass(Text.class);
		        matrixBuilderJob.setMapOutputValueClass(Text.class);
		        
		        // Set the reducer
		        matrixBuilderJob.setReducerClass(DestinationReducer.class);
		        matrixBuilderJob.setOutputKeyClass(Text.class);
		        matrixBuilderJob.setOutputValueClass(Text.class);
		        
		        FileInputFormat.addInputPath(
		        		matrixBuilderJob, new Path(outlinkIdReplacedPath));
		        
		        FileOutputFormat.setOutputPath(
		        		matrixBuilderJob, new Path(sparseMatrixPath));
		        
		        matrixBuilderJob.waitForCompletion(true);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
