package com.neu.pdp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neu.pdp.resources.Accumulator;
import com.neu.pdp.resources.Result;
import com.neu.pdp.resources.Util;

import com.neu.pdp.calculators.SequentialCalculator;
import com.neu.pdp.calculators.CoarseLockCalculator;
import com.neu.pdp.calculators.FineLockCalculator;
import com.neu.pdp.calculators.NoLockCalculator;
import com.neu.pdp.calculators.NoSharingCalculator;

/**
 * CS 6240 - Parallel Data Processing
 * Assignment 1 - Problem 1: Average TMAX calculator
 * Contains the calls to all versions of the program
 * @author Deepak Krishnan
 * @email krishnan.d@husky.neu.edu
 */
public class App 
{
	private static final Logger logger = LogManager.getLogger(
			App.class.getName());
	
	private static boolean addDelay = false;
	
	private static void executeSequentialCalculator(
			List<String> lstWeatherData) {
		// Local variables
		HashMap<String, Float> hmAvgReadingByStation;
		Result sequentialCalculatorResult = new Result();
		SequentialCalculator sc;
		long lStartTime, lEndTime;
		
		System.out.println("---------------------");
		System.out.println("Sequential Calculator");
		System.out.println("---------------------");
		
		for (int i = 0; i < 10; i++) {
			logger.info("Executing sequential calculator. Cycle: " + i);
			
			lStartTime = System.currentTimeMillis(); // Start Timer
    		sc = new SequentialCalculator(addDelay);
			hmAvgReadingByStation = sc.calculate(lstWeatherData);    		
    		lEndTime = System.currentTimeMillis(); // End Timer
    		
    		System.out.println("**** Cycle " + i + " ****");
    		//Util.printAverageTMaxByStation(hmAvgReadingByStation);
    		sequentialCalculatorResult
    			.addExecutionTime(lEndTime - lStartTime);
    		
    		logger.info("Completing sequential calculator. Cycle: " + i);
    		
    		// Explicitly marking for garbage collection
    		hmAvgReadingByStation = null;
		}
		
		// Update the result object with min, max and average execution
		// times
		System.out.println("**** Sequential Result ****");
		sequentialCalculatorResult.updateExecutionTimes();
		System.out.println("Minimum execution time: " + 
				sequentialCalculatorResult.getMinExecutionTime());
		System.out.println("Maximum execution time: " +
				sequentialCalculatorResult.getMaxExecutionTime());
		System.out.println("Average execution time:" +
				sequentialCalculatorResult.getAvgExecutionTime());
		
		// Write result to log
		logger.info("**** Sequential Calculator Result ****");
		logger.info("Minimum execution time: " + 
				sequentialCalculatorResult.getMinExecutionTime());
		logger.info("Maximum execution time: " +
				sequentialCalculatorResult.getMaxExecutionTime());
		logger.info("Average execution time:" +
				sequentialCalculatorResult.getAvgExecutionTime());
		
	}
	
	private static void executeNoLockCalculator(
			List<String> lstWeatherData) {
		// Local variables
		HashMap<String, Float> hmAvgReadingByStation;
    	HashMap<String, Accumulator> hmReadingsByStationId;
		Result noLockCalculatorResult = new Result();
		long lStartTime, lEndTime;
		NoLockCalculator n1, n2;
		
		System.out.println("------------------");
		System.out.println("No Lock Calculator");
		System.out.println("------------------");
		
		for (int i = 0; i < 10; i++) {
			logger.info("Executing no-lock calculator. Cycle: " + i);
			
			lStartTime = System.currentTimeMillis(); // Start Timer
			
    		hmReadingsByStationId = 
        			new HashMap<String, Accumulator>();
    		
    		n1 = new NoLockCalculator(
    				"Thread 1", 
    				lstWeatherData.subList(0, lstWeatherData.size() / 2),
    				hmReadingsByStationId,
    				addDelay);
    		n1.start();
    		
    		n2 = new NoLockCalculator(
    				"Thread 2", 
    				lstWeatherData.subList(
    						(lstWeatherData.size() / 2) + 1, 
    						lstWeatherData.size()), 
    				hmReadingsByStationId,
    				addDelay);
    		n2.start();
    		
    		// Wait for both the threads to complete execution
    		try {
				n1.getThreadObject().join();
				n2.getThreadObject().join();
				
				// Calculate the average reading for each station
	    		hmAvgReadingByStation = Util.getAverageTMaxByStation(
	    				hmReadingsByStationId);
	    		
	    		lEndTime = System.currentTimeMillis(); // End Timer
	    		noLockCalculatorResult
	    			.addExecutionTime(lEndTime - lStartTime);
	    		
	    		System.out.println("**** Cycle " + i + " ****");
	    		//Util.printAverageTMaxByStation(hmAvgReadingByStation);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		    		    		
    		// Explicitly marking for garbage collection
    		hmAvgReadingByStation = null;
    		hmReadingsByStationId = null;
    		n1 = null;
    		n2 = null;
    		
    		logger.info("Completing no-lock calculator. Cycle: " + i);
		}
		
		// Update the result object with min, max and average execution
		// times
		System.out.println("**** No Lock Result ****");
		noLockCalculatorResult.updateExecutionTimes();
		System.out.println("Minimum execution time: " + 
				noLockCalculatorResult.getMinExecutionTime());
		System.out.println("Maximum execution time: " +
				noLockCalculatorResult.getMaxExecutionTime());
		System.out.println("Average execution time:" +
				noLockCalculatorResult.getAvgExecutionTime());
		
		// Write result to log
		logger.info("**** No Lock Calculator Result ****");
		logger.info("Minimum execution time: " + 
				noLockCalculatorResult.getMinExecutionTime());
		logger.info("Maximum execution time: " +
				noLockCalculatorResult.getMaxExecutionTime());
		logger.info("Average execution time:" +
				noLockCalculatorResult.getAvgExecutionTime());
	}
	
	private static void executeCoarseLockCalculator(
			List<String> lstWeatherData) {
		// Local variables
		HashMap<String, Float> hmAvgReadingByStation;
    	HashMap<String, Accumulator> hmReadingsByStationId;
		Result coarseLockCalculatorResult = new Result();
		long lStartTime, lEndTime;
		CoarseLockCalculator c1, c2;
		
		System.out.println("----------------------");
		System.out.println("Coarse Lock Calculator");
		System.out.println("----------------------");
		
		for (int i = 0; i < 10; i++) {
			logger.info("Executing coarse-lock calculator. Cycle: " + i);
			
			lStartTime = System.currentTimeMillis(); // Start Timer
    		
    		hmReadingsByStationId = 
        			new HashMap<String, Accumulator>();
    		
    		c1 = new CoarseLockCalculator(
    				"Thread 1", 
    				lstWeatherData.subList(0, lstWeatherData.size() / 2),
    				hmReadingsByStationId,
    				addDelay);
    		c1.start();
    		
    		c2 = new CoarseLockCalculator(
    				"Thread 2", 
    				lstWeatherData.subList(
    						(lstWeatherData.size() / 2) + 1, 
    						lstWeatherData.size()), 
    				hmReadingsByStationId,
    				addDelay);
    		c2.start();
    		
    		// Wait for both the threads to complete execution
    		try {
				c1.getThreadObject().join();
				c2.getThreadObject().join();
				
				// Calculate the average reading for each station
	    		hmAvgReadingByStation = Util.getAverageTMaxByStation(
	    				hmReadingsByStationId);
	    		
	    		lEndTime = System.currentTimeMillis(); // End Timer
	    		coarseLockCalculatorResult
	    			.addExecutionTime(lEndTime - lStartTime);
	    		
	    		System.out.println("**** Cycle " + i + " ****");
	    		//Util.printAverageTMaxByStation(hmAvgReadingByStation);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		
    		// Explicitly marking for garbage collection
    		hmAvgReadingByStation = null;
    		hmReadingsByStationId = null;
    		c1 = null;
    		c2 = null;
    		
    		logger.info("Completing coarse-lock calculator. Cycle: " + i);			
		}
		
		// Update the result object with min, max and average execution
		// times
		System.out.println("**** Coarse Lock Result ****");
		coarseLockCalculatorResult.updateExecutionTimes();
		System.out.println("Minimum execution time: " + 
				coarseLockCalculatorResult.getMinExecutionTime());
		System.out.println("Maximum execution time: " +
				coarseLockCalculatorResult.getMaxExecutionTime());
		System.out.println("Average execution time:" +
				coarseLockCalculatorResult.getAvgExecutionTime());
		
		// Write result to log
		logger.info("**** Coarse Lock Calculator Result ****");
		logger.info("Minimum execution time: " + 
				coarseLockCalculatorResult.getMinExecutionTime());
		logger.info("Maximum execution time: " +
				coarseLockCalculatorResult.getMaxExecutionTime());
		logger.info("Average execution time:" +
				coarseLockCalculatorResult.getAvgExecutionTime());
	}
	
	private static void executeFineLockCalculator(
			List<String> lstWeatherData) {
		// Local variables
		HashMap<String, Float> hmAvgReadingByStation;
    	HashMap<String, Accumulator> hmReadingsByStationId;
		Result fineLockCalculatorResult = new Result();
		long lStartTime, lEndTime;
		FineLockCalculator f1, f2;
		
		System.out.println("--------------------");
		System.out.println("Fine Lock Calculator");
		System.out.println("--------------------");
		
		for (int i = 0; i < 10; i++) {
			logger.info("Calling fine-lock average calculator");
			
			lStartTime = System.currentTimeMillis(); // Start Timer
    		
    		hmReadingsByStationId = 
        			new HashMap<String, Accumulator>();
    		
    		f1 = new FineLockCalculator(
    				"Thread 1", 
    				lstWeatherData.subList(0, lstWeatherData.size() / 2),
    				hmReadingsByStationId,
    				addDelay);
    		f1.start();
    		
    		f2 = new FineLockCalculator(
    				"Thread 2", 
    				lstWeatherData.subList(
    						(lstWeatherData.size() / 2) + 1, 
    						lstWeatherData.size()), 
    				hmReadingsByStationId,
    				addDelay);
    		f2.start();
    		
    		// Wait for both the threads to complete execution
    		try {
				f1.getThreadObject().join();
				f2.getThreadObject().join();
				
				// Calculate the average reading for each station
	    		hmAvgReadingByStation = Util.getAverageTMaxByStation(
	    				hmReadingsByStationId);
	    		
	    		lEndTime = System.currentTimeMillis(); // End Timer
	    		fineLockCalculatorResult
	    			.addExecutionTime(lEndTime - lStartTime);
	    		
	    		System.out.println("**** Cycle " + i + " ****");
	    		//Util.printAverageTMaxByStation(hmAvgReadingByStation);	    		
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		
    		// Explicitly marking for garbage collection
    		hmAvgReadingByStation = null;
    		hmReadingsByStationId = null;
    		f1 = null;
    		f2 = null;
    		
    		logger.info("Completing fine-lock average calculator");
		}
		
		// Update the result object with min, max and average execution
		// times
		System.out.println("**** Fine Lock Result ****");
		fineLockCalculatorResult.updateExecutionTimes();
		System.out.println("Minimum execution time: " + 
				fineLockCalculatorResult.getMinExecutionTime());
		System.out.println("Maximum execution time: " +
				fineLockCalculatorResult.getMaxExecutionTime());
		System.out.println("Average execution time:" +
				fineLockCalculatorResult.getAvgExecutionTime());
		
		// Write result to log
		logger.info("**** Fine Lock Calculator Result ****");
		logger.info("Minimum execution time: " + 
				fineLockCalculatorResult.getMinExecutionTime());
		logger.info("Maximum execution time: " +
				fineLockCalculatorResult.getMaxExecutionTime());
		logger.info("Average execution time:" +
				fineLockCalculatorResult.getAvgExecutionTime());
	}
	
	private static void executeNoSharingCalculator(
			List<String> lstWeatherData) {
		// Local variables
		HashMap<String, Float> hmAvgReadingByStation;
		Result noSharingCalculatorResult = new Result();
		long lStartTime, lEndTime;
		NoSharingCalculator ns1, ns2;
		
		System.out.println("---------------------");
		System.out.println("No Sharing Calculator");
		System.out.println("---------------------");
		
		for (int i = 0; i < 10; i++) {
			logger.info("Calling no-sharing calculator. Cycle: " + i);
			
			lStartTime = System.currentTimeMillis(); // Start Timer
    		
    		ns1 = new NoSharingCalculator(
    				"Thread 1", 
    				lstWeatherData.subList(0, lstWeatherData.size() / 2),
    				addDelay);
    		ns1.start();
    		
    		ns2 = new NoSharingCalculator(
    				"Thread 2", 
    				lstWeatherData.subList(
    						(lstWeatherData.size() / 2) + 1, 
    						lstWeatherData.size()),
    				addDelay);
    		ns2.start();
    		
    		// Wait for both the threads to complete execution
    		try {
				ns1.getThreadObject().join();
				ns2.getThreadObject().join();
				
				// Combine the data from all threads into primary
				// thread (ns1 in our case)
				List<HashMap<String,Accumulator>> lstData =
						new ArrayList<HashMap<String,Accumulator>>();
				lstData.add(ns2.getTMaxReadingsByStation());
				
				hmAvgReadingByStation = 
						ns1.getAverageReadingByStationId(lstData);
				
				lEndTime = System.currentTimeMillis(); // End Timer
				noSharingCalculatorResult
	    			.addExecutionTime(lEndTime - lStartTime);
	    		
				System.out.println("**** Cycle " + i + " ****");
	    		//Util.printAverageTMaxByStation(hmAvgReadingByStation);	    		
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		
    		// Explicitly marking for garbage collection
    		hmAvgReadingByStation = null;
    		ns1 = null;
    		ns2 = null;
    		
    		logger.info("Completing no-sharing calculator. Cycle: " + i);
		}
		
		// Update the result object with min, max and average execution
		// times
		System.out.println("**** No Sharing Result ****");
		noSharingCalculatorResult.updateExecutionTimes();
		System.out.println("Minimum execution time: " + 
				noSharingCalculatorResult.getMinExecutionTime());
		System.out.println("Maximum execution time: " +
				noSharingCalculatorResult.getMaxExecutionTime());
		System.out.println("Average execution time:" +
				noSharingCalculatorResult.getAvgExecutionTime());
		
		// Write result to log
		logger.info("**** No Sharing Calculator Result ****");
		logger.info("Minimum execution time: " + 
				noSharingCalculatorResult.getMinExecutionTime());
		logger.info("Maximum execution time: " +
				noSharingCalculatorResult.getMaxExecutionTime());
		logger.info("Average execution time:" +
				noSharingCalculatorResult.getAvgExecutionTime());
	}
	
	/**
	 * Main method for this project
	 * @param args [0] - Path of the file containing weather data,
	 * [1] - Boolean flag indicating whether the timer needs to be
	 * extended with fibonacci calculator
	 */
    public static void main( String[] args )
    {
    	logger.info("Entering the main method");
    	
    	// Local variables
    	if (args.length == 0 || args.length > 2) {
    		logger.error("Invalid arguments");
    		System.out.println("Your are missing a required argument. "
    				+ "Expected format: java -jar <jar path> "
    				+ "<weather data file path> "
    				+ "<[optional] add delay flag (true/false)>");
    		System.exit(-1);
    	}
    	else if (args.length == 2) {
    		logger.info("Adding delay to the calculation");
    		addDelay = Boolean.parseBoolean(args[1]);
    	}
    	
    	List<String> lstWeatherData = Util.readCSVFile(args[0]);
    	
    	if (!lstWeatherData.isEmpty() &&
    			lstWeatherData.size() > 0) {
    		// Step 1: Sequential execution
    		executeSequentialCalculator(lstWeatherData);
    		
    		// Step 2: No-lock execution
    		executeNoLockCalculator(lstWeatherData);
    		
    		// Step 3: Coarse lock version
    		executeCoarseLockCalculator(lstWeatherData);
    		
    		// Step 4: Fine lock version
    		executeFineLockCalculator(lstWeatherData);
    		
    		// Step 5: No sharing version
    		executeNoSharingCalculator(lstWeatherData);
    	} else {
    		System.out.println("Empty file supplied");
    	}
    	
    	logger.info("Exiting the main method");
    }
} 
