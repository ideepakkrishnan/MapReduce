/**
 * 
 */
package com.neu.pdp.resources;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class for the project which contains all helper
 * functions
 * @author ideepakkrishnan
 */
public class Util {
	
	private static final Logger logger = LogManager.getLogger(
			Util.class.getName());
	
	/**
	 * Reads the contents stored in a .csv.gz file and returns
	 * it as a List of strings, where each element corresponds
	 * to a line in the CSV file.
	 * @param path Path where the input file is stored
	 * @return A List of String-s
	 */
	@SuppressWarnings("resource")
	public static List<String> readCSVFile(String path) {
		logger.info("Entering readCSVFile method");
		// Local variables
		String line;
		
		// A List object will be used to store the data since
		// the size of the data is dynamic
		List<String> lstWeatherData = new ArrayList<String>();
		
		try {
			// Initialize a GZIP input stream to access the file
			GZIPInputStream gisInput = new GZIPInputStream(
					new FileInputStream(path));
			
			// Initialize a buffer reader to read the input data
			BufferedReader br = new BufferedReader(
					new InputStreamReader(gisInput));
			
			// Read the data from the input file 
			logger.info("Reading data from file");
			while ((line = br.readLine()) != null) {
				lstWeatherData.add(line);
			}
			logger.info("Completed reading data from file");
		} catch (FileNotFoundException e) {
			logger.error(e.getStackTrace());
		} catch (IOException e) {
			logger.error(e.getStackTrace());
		}
		
		logger.info("Returning from readCSVFile method");
		return lstWeatherData;
	}
	
	/**
	 * Calculates the average of an array of integers and returns
	 * it to the caller
	 * @param accumulator A data structure containing the sum and 
	 * count of temparature readings
	 * @return Average value as a float
	 */
	public static float findAverage(Accumulator accumulator) {
		return (float) accumulator.getSum() / accumulator.getCount();
	}
	
	/**
	 * Calculates the TMAX average for each station and returns
	 * the result as a HashMap
	 * @param hmReadingsByStation HashMap with TMAX readings
	 * grouped by station id
	 * @return A HashMap whose key is the station id and the
	 * value is calculated average TMAX value
	 */
	public static HashMap<String, Float> getAverageTMaxByStation(
			HashMap<String, Accumulator> hmReadingsByStation) {
		logger.info("Entering getAverageTMaxByStation method");
		
		// Local variables
		HashMap<String, Float> hmAvgReadingByStation = 
				new HashMap<String, Float>();
		
		// Process each station
		for (Map.Entry<String, Accumulator> entry: 
			hmReadingsByStation.entrySet()) {
			hmAvgReadingByStation.put(
					entry.getKey(), 
					findAverage(entry.getValue()));
		}
		
		logger.info("Returning from getAverageTMaxByStation method");
		return hmAvgReadingByStation;
	}
	
	/**
	 * Prints the average TMAX reading for each station
	 * @param hmAvgReadingByStation A HashMap whose key is station
	 * id and value is the average TMAX reading
	 */
	public static void printAverageTMaxByStation(
			HashMap<String, Float> hmAvgReadingByStation) {
		logger.info("Entering printAverageTMaxByStation method");
		
		for (Map.Entry<String, Float> entry: 
			hmAvgReadingByStation.entrySet()) {
			System.out.println(
					String.format(
							"Station Id: %s, Average TMAX: %.2f",
							entry.getKey(),
							entry.getValue()));
		}
		
		logger.info("Returning from printAverageTMaxByStation method");
	}
	
	/**
	 * Returns the smallest value from a list of Long-s
	 * @param data A list storing Long values
	 * @return The smallest entry in the list
	 */
	public static Long findMinimumExecutionTime(
			List<Long> data) {
		logger.info("Entering findMinimum method");
		
		// Local variables
		Long lMin = null;
		
		// Iterate through the list and find the minimum value
		for (Long item: data) {
			if (lMin == null) {
				lMin = item;
			} else {
				lMin = (lMin <= item) ? lMin : item;
			}
		}
		
		logger.info("Returning from findMinimum method");
		return lMin;
	}
	
	/**
	 * Returning the largest value from a list of Long-s
	 * @param data A list storing Long values
	 * @return The largest entry in the list
	 */
	public static Long findMaximumExecutionTime(
			List<Long> data) {
		logger.info("Entering findMaximum method");
		
		// Local variables
		Long lMax = null;
		
		// Iterate through the list and find the maximum value
		for (Long item: data) {
			if (lMax == null) {
				lMax = item;
			} else {
				lMax = (lMax >= item) ? lMax : item;
			}
		}
		
		logger.info("Returning from findMaximum method");
		return lMax;
	}
	
	/**
	 * Returns the average value calculated from a list of
	 * Long-s
	 * @param data A list of Long values
	 * @return The calculated average
	 */
	@SuppressWarnings("null")
	public static Long findAverageExecutionTime(
			List<Long> data) {
		logger.info("Entering findAverage method");
		
		// Local variables
		Long lSum = null;
		
		// Iterate through the list and find the maximum value
		for (Long item: data) {
			if (lSum == null) {
				lSum = item;
			} else {
				lSum += item;
			}
		}
		
		logger.info("Returning from findAverage method");
		return (lSum == null) ? lSum : (lSum / data.size());
	}
	
	/**
	 * Generates a fibonacci series of specified length and
	 * returns it as a list of integers to the caller
	 * @param size Required length of the sequence
	 * @return The next number in the sequence
	 */
	public static int fibonacci(int n) {
		if (n == 0) {
			return 0;
		} else if (n == 1) {
			return 1;
		} else {
			return fibonacci(n - 1) + fibonacci(n - 2);
		}
	}
	
}
