/**
 * 
 */
package com.neu.pdp.calculators;

import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neu.pdp.resources.Util;

/**
 * Class containing methods to sequentially process the TMAX
 * temperature readings grouped by station id and calculate
 * their average reading
 * @author ideepakkrishnan
 */
public class SequentialCalculator {
	
	private static final Logger logger = LogManager.getLogger(
			SequentialCalculator.class.getName());
	
	/**
	 * Calculates the average TMAX temperature for each station
	 * id and returns the result as a HashMap whose key is the
	 * station id and value is the calculated average
	 * @param lstWeatherData List of Strings
	 * @return A HashMap whose key is a String representing the
	 * station id and whose value is a float representing the
	 * associated average TMAX temperature
	 */
	public static HashMap<String, Float> calculate(
			List<String> lstWeatherData) {
		logger.info("Entering sequential method");
		
		// Filter out the required entries from the data passed
		// in as argument and group the TMAX readings by station
		// id 
		HashMap<String, List<Integer>> hmFilteredData = 
				Util.getTMaxReadingsByStation(lstWeatherData);
		
		// Calculate the average for each station and return it
		// to the caller		
		logger.info("Returning from sequential method");
		return Util.getAverageTMaxByStation(hmFilteredData);
	}
}
