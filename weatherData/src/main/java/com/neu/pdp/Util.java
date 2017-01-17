/**
 * 
 */
package com.neu.pdp;

import java.util.List;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

/**
 * Utility class for the project which contains all helper
 * functions
 * 
 * @author ideepakkrishnan
 * 
 */
public class Util {
	
	/**
	 * Reads the contents stored in a .csv.gz file and returns
	 * it as an array of strings, where each element corresponds
	 * to a line in the CSV file, to the caller.
	 * @param path Path where the input file is stored
	 * @return An array of String-s
	 */
	public static String[] readCSVFile(String path) {
		// Local variables
		String line;
		
		// A List object will be used to store the data since
		// the size of the data is dynamic. The contents will
		// be converted to a String array while being passed
		// on to the caller.
		List<String> lstWeatherData = new ArrayList<String>();
		
		try {
			// Read the data from the input file
			GZIPInputStream gisInput = new GZIPInputStream(
					new FileInputStream(path));
			
			// Initialize a buffer reader to read the input data
			BufferedReader br = new BufferedReader(
					new InputStreamReader(gisInput));
			
			while ((line = br.readLine()) != null) {
				lstWeatherData.add(line);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// While returning we need to convert the List of String-s
		// into an array of String-s
		return lstWeatherData.toArray(new String[lstWeatherData.size()]);
	}
}
