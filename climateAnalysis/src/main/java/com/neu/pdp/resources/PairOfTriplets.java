/**
 * 
 */
package com.neu.pdp.resources;

/**
 * Defines a pair of integer triplets
 * @author ideepakkrishnan
 */
public class PairOfTriplets {
	
	private IntTriplet first;
	private IntTriplet second;
	
	/**
	 * Default constructor
	 */
	public PairOfTriplets() {
		first = new IntTriplet();
		second = new IntTriplet();
	}
	
	/**
	 * Overloaded constructor
	 * @param first
	 * @param second
	 */
	public PairOfTriplets(IntTriplet first, IntTriplet second) {
		this.first = first;
		this.second = second;
	}
	
	/**
	 * @return the first
	 */
	public IntTriplet getFirst() {
		return first;
	}
	/**
	 * @param first the first to set
	 */
	public void setFirst(IntTriplet first) {
		this.first = first;
	}
	/**
	 * @return the second
	 */
	public IntTriplet getSecond() {
		return second;
	}
	/**
	 * @param second the second to set
	 */
	public void setSecond(IntTriplet second) {
		this.second = second;
	}

}
