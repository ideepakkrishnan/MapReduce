package com.neu.pdp;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class FileReader {
	
	private static HashMap<String, Accumulator> hmTmaxByStationId;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Inside main function");
		List<String> lstWeatherData = readCSVFile(args[0]);
		hmTmaxByStationId = new HashMap<String, Accumulator>();
		groupTMaxReadingsByStation(lstWeatherData);
		System.out.println("Exiting main function");

	}
	
	private static List<String> readCSVFile(String path) {		
		// Local variables
		String line;
		
		// A List object will be used to store the data since
		// the size of the data is dynamic
		List<String> lstWeatherData = new ArrayList<String>();
		
		System.out.println("Starting file read at: " + System.currentTimeMillis());
		
		try {
			// Initialize a GZIP input stream to access the file
			GZIPInputStream gisInput = new GZIPInputStream(
					new FileInputStream(path));
			
			// Initialize a buffer reader to read the input data
			BufferedReader br = new BufferedReader(
					new InputStreamReader(gisInput));
			
			// Read the data from the input file
			while ((line = br.readLine()) != null) {
				lstWeatherData.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Completing file read at: " + System.currentTimeMillis());
		
		return lstWeatherData;
	}
	
	private static void groupTMaxReadingsByStation(
			List<String> lstWeatherData) {
		// Local variables
		String[] strArrData;
		
		System.out.println("Entering record parsing");
		System.out.println(System.currentTimeMillis());
		
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
							false);
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
		
		System.out.println(System.currentTimeMillis());
		System.out.println("Completing record parsing");
	}

}
