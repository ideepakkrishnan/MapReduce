/**
 * 
 */
package com.neu.pdp.resources;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Define a pair of integers that are writable.
 * They are serialized in a byte comparable format.
 * @author ideepakkrishnan
 */
public class IntPair implements Writable {
	private int first;
	private int second;
	
	/**
	 * Default constructor
	 */
	public IntPair() {
		first = 0;
		second = 0;
	}
	
	/**
	 * Overloaded Constructor
	 * @param first Integer
	 * @param second Integer
	 */
	public IntPair(int first, int second) {
		this.first = first;
		this.second = second;
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
	 * Update the values stored in local variables
	 * @param first Integer
	 * @param second Integer
	 */
	public void set(int first, int second) {
		this.first = first;
		this.second = second;
	}

	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}
	
}
