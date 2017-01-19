package com.neu.pdp;

import java.util.HashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neu.pdp.calculators.SequentialCalculator;
import com.neu.pdp.calculators.NoLockCalculator;

/**
 * Contains the calls to all versions of the program
 * @author ideepakkrishnan
 */
public class App 
{
	private static final Logger logger = LogManager.getLogger(
			App.class.getName());
	
	/**
	 * Main method for this project
	 * @param args
	 */
    public static void main( String[] args )
    {
    	logger.info("Entering the main method");
    	
    	// Local variables
    	String path = "/home/ideepakkrishnan/Downloads/1763.csv.gz";
    	List<String> lstWeatherData = Util.readCSVFile(path);
    	HashMap<String, Float> hmAvgReadingByStation;
    	HashMap<String, List<Integer>> hmReadingsByStationId = 
    			new HashMap<String, List<Integer>>();
    	
    	if (!lstWeatherData.isEmpty() &&
    			lstWeatherData.size() > 0) {
    		// Step 1: Sequential execution
    		/*logger.info("Data read complete. Calling sequential method");
    		hmAvgReadingByStation = SequentialCalculator.calculate(
    				lstWeatherData);
    		
    		Util.printAverageTMaxByStation(hmAvgReadingByStation);
    		logger.info("Completing sequential average calculator");
    		
    		// Explicitly marking for garbage collection
    		hmAvgReadingByStation = null;*/
    		
    		// Step 2: No-lock execution
    		logger.info("Calling no-lock average calculator");
    		NoLockCalculator n1 = new NoLockCalculator(
    				"Thread 1", 
    				lstWeatherData.subList(0, lstWeatherData.size() / 2),
    				hmReadingsByStationId);
    		n1.start();
    		
    		NoLockCalculator n2 = new NoLockCalculator(
    				"Thread 2", 
    				lstWeatherData.subList(
    						(lstWeatherData.size() / 2) + 1, 
    						lstWeatherData.size()), 
    				hmReadingsByStationId);
    		n2.start();
    		
    		// Wait for both the threads to complete execution
    		try {
				n1.getThreadObject().join();
				n2.getThreadObject().join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		
    		// Calculate the average reading for each station
    		hmAvgReadingByStation = Util.getAverageTMaxByStation(
    				hmReadingsByStationId);
    		
    		Util.printAverageTMaxByStation(hmAvgReadingByStation);
    		logger.info("Completing no-lock average calculator");
    		
    		// Explicitly marking for garbage collection
    		hmAvgReadingByStation = null;
    		n1 = null;
    		n2 = null;
    	}
    	
    	logger.info("Exiting the main method");
    }
}
