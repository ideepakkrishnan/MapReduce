/**
 * 
 */
package com.neu.pdp.resources;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Define a triplet of integers that are writable.
 * They are serialized in a byte comparable format.
 * @author ideepakkrishnan
 */
public class IntTriplet implements Writable {
	private int first;
	private int second;
	private int third;
	
	/**
	 * Default constructor
	 */
	public IntTriplet() {
		first = 0;
		second = 0;
		third = 0;
	}
	
	/**
	 * Overloaded Constructor
	 * @param first Integer
	 * @param second Integer
	 */
	public IntTriplet(int first, int second, int third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}

	/**
	 * @return the first
	 */
	public int getFirst() {
		return first;
	}

	/**
	 * @param first the first to set
	 */
	public void setFirst(int first) {
		this.first = first;
	}

	/**
	 * @return the second
	 */
	public int getSecond() {
		return second;
	}

	/**
	 * @param second the second to set
	 */
	public void setSecond(int second) {
		this.second = second;
	}	
	
	/**
	 * @return the third
	 */
	public int getThird() {
		return third;
	}

	/**
	 * @param third the third to set
	 */
	public void setThird(int third) {
		this.third = third;
	}

	/**
	 * Update the values stored in local variables
	 * @param first Integer
	 * @param second Integer
	 */
	public void set(int first, int second, int third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}

	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
		third = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
		out.writeInt(third);
	}
	
}
