/**
 * 
 */
package com.neu.pdp.calculators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neu.pdp.resources.Accumulator;
import com.neu.pdp.resources.Util;

/**
 * Class containing methods to concurrently process the
 * temperature readings grouped by station id. Each object
 * is self-containing and the processed data is stored in
 * a member variable. After all threads have completed their
 * execution, the data is combined in one of the threads
 * before we calculate the average reading for each station.
 * @author ideepakkrishnan
 */
public class NoSharingCalculator implements Runnable {
	
	private static final Logger logger = LogManager.getLogger(
			NoSharingCalculator.class.getName());
	
	private Thread t;
	private String threadName;
	private List<String> lstWeatherData;
	private HashMap<String, Accumulator> hmTmaxByStationId;
	private boolean addDelay;
	
	public NoSharingCalculator(
			String threadName, 
			List<String> lstWeatherData,
			boolean addDelay) {
		this.threadName = threadName;
		this.lstWeatherData = lstWeatherData;
		this.hmTmaxByStationId = new HashMap<String, Accumulator>();
		this.addDelay = addDelay;
		logger.info("Creating thread: " + threadName);
	}
	
	/**
	 * Returns a pointer to the Thread object in this class
	 * @return A Thread object
	 */
	public Thread getThreadObject() {
		return t;
	}
	
	/**
	 * Returns the object storing TMAX readings grouped by their
	 * station id 
	 * @return
	 */
	public HashMap<String, Accumulator> getTMaxReadingsByStation() {
		return hmTmaxByStationId;
	}
	
	public void run() {
		logger.info(threadName + ": Entering run method");
		
		// Group readings by station		
		groupTMaxReadingsByStation();
		
		logger.info(threadName + ": Exiting run method");
	}
	
	public void start() {
		logger.info("Starting thread: " + threadName);
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}
	
	/**
	 * Combines a list of HashMap-s each of which stores the 
	 * temperature readings grouped by station id into the local
	 * data structure and calculates the average reading for
	 * each station before passing this calculated result to the
	 * caller.
	 * @param lstData A list of HashMap-s
	 * @return A HashMap whose key is the station id and the
	 * value is calculated average TMAX
	 */
	public HashMap<String, Float> getAverageReadingByStationId(
			List<HashMap<String, Accumulator>> lstData) {
		logger.info(
				threadName + 
				": Entering getAverageReadingByStationId method");
		
		// Combine all data into the local data structure
		for (HashMap<String, Accumulator> hmItem : lstData) {
			combineGroupedData(hmItem);
		}
		
		logger.info(
				threadName + 
				": Exiting getAverageReadingByStationId method");
		
		// Calculate the average reading for each station and
		// return the result to the caller
		return Util.getAverageTMaxByStation(hmTmaxByStationId);
	}
	
	/**
	 * A helper method which accepts a list of HashMap-s and
	 * appends the data from each of them into the local data
	 * structure.
	 * @param hmReadingsByStationId A list of HashMap-s where
	 * each one has station id as key and a list of TMAX
	 * readings as value
	 */
	private void combineGroupedData(
			HashMap<String, Accumulator> hmReadingsByStationId) {
		logger.info(threadName + ": Entering combineGroupedData method");
		
		// Iterate through each entry and append it to the data structure
		// in this object
		for (Map.Entry<String, Accumulator> entry: 
			hmReadingsByStationId.entrySet()) {
			if (hmTmaxByStationId.containsKey(entry.getKey())) {
				hmTmaxByStationId
					.get(entry.getKey())
					.combineValues(
							entry.getValue().getSum(),
							entry.getValue().getCount());
			} else {
				hmTmaxByStationId.put(entry.getKey(), entry.getValue());
			}
		}
		
		logger.info(threadName + ": Exiting combineGroupedData method");
	}
	
	/**
	 * Filters out the TMAX readings, groups them by station id and
	 * returns this data as a HashMap
	 */
	private void groupTMaxReadingsByStation() {
		logger.info(
				threadName + 
				": Entering groupTMaxReadingsByStation method");
		
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
		
		strArrData = null;
		
		logger.info(
				threadName + 
				": Exiting groupTMaxReadingsByStation method");
	}

}
