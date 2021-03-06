/**
 * 
 */
package com.neu.pdp.calculators;

import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neu.pdp.resources.Accumulator;

/**
 * Class containing methods to concurrently process the
 * temperature readings grouped by station id and calculate
 * their average without any synchronization on the updates
 * to the data structure that stores the processed data.
 * @author ideepakkrishnan
 */
public class NoLockCalculator implements Runnable {
	
	private static final Logger logger = LogManager.getLogger(
			NoLockCalculator.class.getName());
	
	private Thread t;
	private String threadName;
	private List<String> lstWeatherData;
	private HashMap<String, Accumulator> hmTmaxByStationId;
	private boolean addDelay;
	
	public NoLockCalculator(
			String threadName, 
			List<String> lstWeatherData,
			HashMap<String, Accumulator> hmTmaxByStationId,
			boolean addDelay) {
		this.threadName = threadName;
		this.lstWeatherData = lstWeatherData;
		this.hmTmaxByStationId = hmTmaxByStationId;
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
	 * Filters out the TMAX readings, groups them by station id and
	 * returns this data as a HashMap
	 * @param lstWeatherData List of String-s where each entry is
	 * comma separated in the format: station id, date, observation
	 * type, observation value, observation time
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

}
