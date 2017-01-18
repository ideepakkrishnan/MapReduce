package com.neu.pdp;

/**
 * Contains the calls to all versions of the program
 * @author ideepakkrishnan
 *
 */
public class App 
{	
	/**
	 * Main method for this project
	 * @param args
	 */
    public static void main( String[] args )
    {
    	try {
	    	String path = "/home/ideepakkrishnan/Downloads/1763.csv.gz";
	    	String[] weatherData = Util.readCSVFile(path);    	
	    	for (String line: weatherData) {
	    		System.out.println(line);
	    	}
    	}
    	catch (Exception e) {
			// TODO: handle exception
    		e.printStackTrace();
		}
    }
}
