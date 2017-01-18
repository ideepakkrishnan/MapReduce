package com.neu.pdp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Contains the calls to all versions of the program
 * @author ideepakkrishnan
 *
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
    	    	
    	String path = "/home/ideepakkrishnan/Downloads/1763.csv.gz";
    	List<String> lstWeatherData = Util.readCSVFile(path);
    	
    	if (!lstWeatherData.isEmpty() &&
    			lstWeatherData.size() > 0) {
    		logger.info("Data read complete. Calling sequential method");
    		HashMap<String, Float> hmAvgReadingByStation = 
    				AverageCalculator.sequential(lstWeatherData);
    		
    		for (Map.Entry<String, Float> entry: 
    			hmAvgReadingByStation.entrySet()) {
    			System.out.println(
    					String.format(
    							"Station Id: %s, Average TMAX: %.2f",
    							entry.getKey(),
    							entry.getValue()));
    		}
    	}
    	
    	logger.info("Exiting the main method");
    }
}
