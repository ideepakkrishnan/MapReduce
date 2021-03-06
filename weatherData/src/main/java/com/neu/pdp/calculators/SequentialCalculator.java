/**
 * 
 */
package com.neu.pdp.calculators;

import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neu.pdp.resources.Accumulator;
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
	
	private HashMap<String, Accumulator> hmTmaxByStationId;
	private boolean addDelay;
	
	public SequentialCalculator(boolean addDelay) {
		this.hmTmaxByStationId = new HashMap<String, Accumulator>();
		this.addDelay = addDelay;
	}
	
	/**
	 * Calculates the average TMAX temperature for each station
	 * id and returns the result as a HashMap whose key is the
	 * station id and value is the calculated average
	 * @param lstWeatherData List of Strings
	 * @return A HashMap whose key is a String representing the
	 * station id and whose value is a float representing the
	 * associated average TMAX temperature
	 */
	public HashMap<String, Float> calculate(
			List<String> lstWeatherData) {
		logger.info("Entering sequential method");
		
		// Filter out the required entries from the data passed
		// in as argument and group the TMAX readings by station
		// id 
		groupTMaxReadingsByStation(lstWeatherData);
		
		// Calculate the average for each station and return it
		// to the caller		
		logger.info("Returning from sequential method");
		return Util.getAverageTMaxByStation(hmTmaxByStationId);
	}
	
	/**
	 * Filters out the TMAX readings, groups them by station id and
	 * returns this data as a HashMap
	 * @param lstWeatherData List of String-s where each entry is
	 * in the format: station id, date, observation type, observation
	 * value, observation time
	 * @return A HashMap with station id as the key and a List of
	 * Integer-s that represent the TMAX readings for the station
	 * as value
	 */
	public void groupTMaxReadingsByStation(
			List<String> lstWeatherData) {
		logger.info("Entering groupTMaxReadingsByStation method");
		
		// Local variables
		String[] strArrData;		
		
		// Iterate through the list of Strings and process each one
		for (String strCurrReading: lstWeatherData) {
			strArrData = strCurrReading.split(",");
			
			// The array follows the format:
			// [station id, date, observation type, observation
			//  value, observation time]
			
			if (strArrData[2].equals("TMAX") && 
					!strArrData[3].isEmpty()) {
				// Check if the station id already exists in the
				// HashMap
				if (hmTmaxByStationId.containsKey(strArrData[0])) {
					// Add the current TMAX reading into the list
					hmTmaxByStationId.get(strArrData[0]).addValue(
							Integer.parseInt(strArrData[3]),
							addDelay);
				} else {
					// We need to initialize a new key-value pair
					// for this new station
					hmTmaxByStationId.put(
							strArrData[0], 
							new Accumulator(
									Integer.parseInt(strArrData[3])));
				}
			}
		}
		
		logger.info("Returning from groupTMaxReadingsByStation method");
	}
}
