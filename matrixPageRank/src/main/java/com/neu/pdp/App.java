package com.neu.pdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.neu.pdp.pageRank.core.RowPartitioner;
import com.neu.pdp.pageRank.core.danglerHandler.DanglerIdFirstLetterPartitioner;
import com.neu.pdp.pageRank.core.danglerHandler.DanglerNameMapper;
import com.neu.pdp.pageRank.core.danglerHandler.DanglerRankReducer;
import com.neu.pdp.pageRank.preProcessor.SourceIdJoiner.SourceIdReducer;
import com.neu.pdp.pageRank.preProcessor.SourceIdJoiner.SourceMapper;
import com.neu.pdp.pageRank.preProcessor.adjacencyListBuilder.AdjacencyListReducer;
import com.neu.pdp.pageRank.preProcessor.adjacencyListBuilder.GroupComparator;
import com.neu.pdp.pageRank.preProcessor.adjacencyListBuilder.TokenizerMapper;
import com.neu.pdp.pageRank.preProcessor.adjacencyListBuilder.ValuePartitioner;
import com.neu.pdp.pageRank.preProcessor.matrixBuilder.FirstLetterPartitioner;
import com.neu.pdp.pageRank.preProcessor.matrixBuilder.PageIdGoupingComparator;
import com.neu.pdp.pageRank.preProcessor.matrixBuilder.PageNameMapper;
import com.neu.pdp.pageRank.preProcessor.matrixBuilder.RecordTypeSortingComparator;
import com.neu.pdp.pageRank.preProcessor.sparseMatrixBuilder.DestinationMapper;
import com.neu.pdp.pageRank.preProcessor.sparseMatrixBuilder.DestinationReducer;
import com.neu.pdp.pageRank.resources.CondensedNode;
import com.neu.pdp.pageRank.resources.KeyPair;
import com.neu.pdp.pageRank.resources.SourceRankPair;
import com.neu.pdp.pageRank.topK.NodeRankMapper;
import com.neu.pdp.pageRank.topK.TopKReducer;

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
    		
    		if (args.length != 13) {
    			System.out.println("Invalid argument list found. Please retry.");
    			System.exit(-1);
    		} else {
    			
    			String wikiDataPath = args[0];
    			String adjListPath = args[1];
    			String pageIdMapPath = args[2];
    			String sourceIdReplacedPath = args[3];
    			String outlinkIdReplacedPath = args[4];
    			String splitRankFilesPath = args[5];
    			String mergedRankPath = args[6];
    			String mergedRankFile = mergedRankPath + "/ranks";
    			String danglingNodesPath = args[7];
    			String sparseMatrixPath = args[8];
    			String mergedPageIdMapPath = args[9];
    			String mergedPageIdMapFile = mergedPageIdMapPath + "/pageIdMapping";
    			String top100Path = args[10];
    			String tempPath = args[11];
    			String partitionType = args[12];
    			
    			boolean deleteSplitRank = true;
    			boolean deletePageIdMapPathFiles = true;
    			
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
		        sourceIdMapperConf.set("defaultRankPath", splitRankFilesPath);
		        sourceIdMapperConf.set("danglingNodesPath", danglingNodesPath);
		        
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
		        
		        /**
		         *  Merge the page rank files into a single file
		         */
		        
		        Configuration fileMergeConf = new Configuration();
		        FileSystem fs = null;
		        
		        try {
		        	fs = FileSystem.get(fileMergeConf);
		        	FileUtil.copyMerge(
		        			fs, new Path(splitRankFilesPath), 
		        			fs, new Path(mergedRankFile), 
		        			deleteSplitRank, fileMergeConf, null);
		        } catch (Exception e) {
		        	System.out.println("Error while merging rank files");
		        	e.printStackTrace();
		        }
		        
		        /**
		         * Initialize matrix
		         */
		        
		        Configuration matrixGeneratorConfig = new Configuration();
		        matrixGeneratorConfig.setLong("pageCount", pageCount);
		        
		        Job matrixGeneratorJob = Job.getInstance(
		        		matrixGeneratorConfig, "Matrix Generator");
		        matrixGeneratorJob.setJarByClass(App.class);
		        
		        // Set the mapper
		        matrixGeneratorJob.setMapperClass(PageNameMapper.class);
		        matrixGeneratorJob.setMapOutputKeyClass(CondensedNode.class);
		        matrixGeneratorJob.setMapOutputValueClass(SourceRankPair.class);
		        
		        // Set the intermediate classes
		        matrixGeneratorJob.setPartitionerClass(FirstLetterPartitioner.class);
		        matrixGeneratorJob.setSortComparatorClass(RecordTypeSortingComparator.class);
		        matrixGeneratorJob.setGroupingComparatorClass(PageIdGoupingComparator.class);
		        		        
		        // Set the reducer's output key and value types
		        if (partitionType.equalsIgnoreCase("row")) {
		        	matrixGeneratorJob.setReducerClass(
		        			com.neu.pdp.pageRank.preProcessor.matrixBuilder.rowVersion.PageNameIdReducer.class);
		        } else {
		        	matrixGeneratorJob.setReducerClass(
		        			com.neu.pdp.pageRank.preProcessor.matrixBuilder.colVersion.PageNameIdReducer.class);
		        }
		        matrixGeneratorJob.setOutputKeyClass(LongWritable.class);
		        matrixGeneratorJob.setOutputValueClass(Text.class);
		        
		        FileInputFormat.addInputPath(matrixGeneratorJob, new Path(pageIdMapPath));
		        FileInputFormat.addInputPath(matrixGeneratorJob, new Path(sourceIdReplacedPath));
		        FileOutputFormat.setOutputPath(
		        		matrixGeneratorJob, new Path(outlinkIdReplacedPath));
		        
		        matrixGeneratorJob.waitForCompletion(true);
		        
		        /**
		         * Build the sparse matrix
		         */
		        
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
		        
		        /**
		         * Page rank calculation
		         */		        
		        for (; i < 10; i++) {
		        	System.out.println("Iteration: " + i);
		        	
		        	/**
		        	 * Calculate page rank for this iteration
		        	 */
		        	
			        Configuration rankCalculatorConfig = new Configuration();
			        rankCalculatorConfig.setDouble("alpha", 0.85);
			        rankCalculatorConfig.setDouble("delta", currDelta);
			        rankCalculatorConfig.setLong("pageCount", pageCount);
			        rankCalculatorConfig.set("rankFilePath", mergedRankFile);
			        
			        Job rankCalculatorJob = Job.getInstance(
			        		rankCalculatorConfig, "Page Rank Calculator");		        
			        rankCalculatorJob.addCacheFile(
			        		new Path(mergedRankFile).toUri());
			        rankCalculatorJob.setJarByClass(App.class);
			        
			        // Set the mapper
			        if (partitionType.equals("row")) {
			        	rankCalculatorJob.setMapperClass(
			        			com.neu.pdp.pageRank.core.rowVersion.RowColumnMapper.class);
			        } else {
			        	rankCalculatorJob.setMapperClass(
			        			com.neu.pdp.pageRank.core.colVersion.RowColumnMapper.class);
			        }
			        rankCalculatorJob.setMapOutputKeyClass(LongWritable.class);
			        rankCalculatorJob.setMapOutputValueClass(SourceRankPair.class);
			        
			        // Set intermediate classes
			        rankCalculatorJob.setPartitionerClass(RowPartitioner.class);
			        
			        // Set the reducer
			        if (partitionType.equals("row")) {
			        	rankCalculatorJob.setReducerClass(
			        			com.neu.pdp.pageRank.core.rowVersion.RowColumnReducer.class);
			        } else {
			        	rankCalculatorJob.setReducerClass(
			        			com.neu.pdp.pageRank.core.colVersion.RowColumnReducer.class);
			        }
			        rankCalculatorJob.setOutputKeyClass(LongWritable.class);
			        rankCalculatorJob.setOutputValueClass(DoubleWritable.class);
			        
			        FileInputFormat.addInputPath(
			        		rankCalculatorJob, new Path(sparseMatrixPath));
			        
			        Path currIterationOutputPath = new Path(splitRankFilesPath);
			        
			        FileOutputFormat.setOutputPath(
			        		rankCalculatorJob, 
			        		currIterationOutputPath);
			        
			        rankCalculatorJob.waitForCompletion(true);
			        
			        if (fs != null) {
				        try {
				        	// Delete the ranks stored from previous iteration
				        	FileUtil.fullyDelete(fs, new Path(mergedRankPath));
				        	
				        	// Merge the ranks generated in current iteration
				        	// into a single file so that it is easier to
				        	// distribute using distributed file cache. Once
				        	// merged, delete the folder where split files
				        	// from current iteration's output is stored since
				        	// it is no longer needed.
				        	// TODO Search for a way to compress the merged file
				        	// before distributing.
				        	FileUtil.copyMerge(
				        			fs, currIterationOutputPath, 
				        			fs, new Path(mergedRankFile), 
				        			deleteSplitRank, fileMergeConf, null);
				        } catch (Exception e) {
				        	System.out.println("Error while merging rank files");
				        	e.printStackTrace();
				        }
			        }
			        
			        /**
			         * Calculate delta for next iteration
			         */
			        
			        Configuration deltaCalculatorConfig = new Configuration();
			        
			        Job deltaCalculatorJob = Job.getInstance(
			        		deltaCalculatorConfig, "Delta Calculator");
			        deltaCalculatorJob.setJarByClass(App.class);
			        
			        // Set the mapper
			        deltaCalculatorJob.setMapperClass(DanglerNameMapper.class);
			        deltaCalculatorJob.setMapOutputKeyClass(CondensedNode.class);
			        deltaCalculatorJob.setMapOutputValueClass(CondensedNode.class);
			        
			        // Set the intermediate classes
			        deltaCalculatorJob.setPartitionerClass(DanglerIdFirstLetterPartitioner.class);
			        deltaCalculatorJob.setSortComparatorClass(RecordTypeSortingComparator.class);
			        deltaCalculatorJob.setGroupingComparatorClass(PageIdGoupingComparator.class);
			        
			        deltaCalculatorJob.setReducerClass(DanglerRankReducer.class);
		        
			        deltaCalculatorJob.setOutputKeyClass(NullWritable.class);
			        deltaCalculatorJob.setOutputValueClass(NullWritable.class);
			        
			        FileInputFormat.addInputPath(deltaCalculatorJob, new Path(mergedRankPath));
			        FileInputFormat.addInputPath(deltaCalculatorJob, new Path(danglingNodesPath));
			        System.out.println(danglingNodesPath);
			        FileOutputFormat.setOutputPath(
			        		deltaCalculatorJob, new Path(tempPath + i));
			        
			        deltaCalculatorJob.waitForCompletion(true);
			        
			        // Update the delta value in the config
			        // for next run
			        Counters counters = deltaCalculatorJob.getCounters();
			        Counter danglingCounter = counters.findCounter(DANGLING_NODES.TOTAL_PAGE_RANK);
			        currDelta = Double.longBitsToDouble(danglingCounter.getValue());
			        
			        System.out.println("Delta: " + String.valueOf(currDelta));
		        }
		        
		        /**
		         *  Merge the page name - page ID mapping files into a single file
		         */
		        
		        fileMergeConf = new Configuration();
		        fs = null;
		        
		        try {
		        	fs = FileSystem.get(fileMergeConf);
		        	FileUtil.copyMerge(
		        			fs, new Path(pageIdMapPath), 
		        			fs, new Path(mergedPageIdMapFile), 
		        			deletePageIdMapPathFiles, fileMergeConf, null);
		        } catch (Exception e) {
		        	System.out.println("Error while merging page name - Id mapping files");
		        	e.printStackTrace();
		        }
		        
		        /**
		         * Top-K
		         */
		        
		        // Execute Top-K job to find the top 100 pages
		        Configuration topKConf = new Configuration();
		        topKConf.set("mapFilePath", mergedPageIdMapFile);
		        
		        Job topKJob = Job.getInstance(topKConf, "Top-100 Pages");
		        topKJob.setJarByClass(App.class);
		        
		        // Set the mapper
		        topKJob.setMapperClass(NodeRankMapper.class);
		        topKJob.setMapOutputKeyClass(NullWritable.class);
		        topKJob.setMapOutputValueClass(SourceRankPair.class);	        
		        
		        // Set the reducer
		        topKJob.setReducerClass(TopKReducer.class);
		        topKJob.setOutputKeyClass(Text.class);
		        topKJob.setOutputValueClass(DoubleWritable.class);
		        topKJob.setNumReduceTasks(1);
	        		        
	        	// Specify input folder for this iteration
        		FileInputFormat.addInputPath(topKJob, new Path(mergedRankPath));
	        	
	        	// Specify output folder for this iteration
		        FileOutputFormat.setOutputPath(topKJob, new Path(top100Path));
		        
		        // Execute the job
		        topKJob.waitForCompletion(true);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
