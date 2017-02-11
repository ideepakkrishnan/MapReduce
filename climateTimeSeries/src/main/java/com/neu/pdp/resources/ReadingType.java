/**
 * 
 */
package com.neu.pdp.resources;

/**
 * Defines the valid temperature reading types
 * supported by this program.
 * @author ideepakkrishnan
 */
public enum ReadingType {

	MIN(0),
	MAX(1),
	INVALID(-1);
	
	private int value;
	
	private ReadingType(int val) {
		this.value = val;
	}
	
	/**
	 * Returns the value associated with the specified
	 * temperature reading type
	 * @return Integer value associated with reading 
	 * type
	 */
	public int getValue() {
		return value;
	}
}
