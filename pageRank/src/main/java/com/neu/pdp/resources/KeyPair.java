/**
 * 
 */
package com.neu.pdp.resources;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * The class that wraps around the composite key being used
 * inside the pre-processor. It implements WritableComparable so
 * that the compare method could be used in KeyComparator and
 * GroupComparator for processing.
 * @author ideepakkrishnan
 *
 */
public class KeyPair implements WritableComparable<KeyPair> {
	
	// Class level attributes
	private Text first;
	private Text second;
	
	/**
	 * @return the first
	 */
	public Text getFirst() {
		return first;
	}

	/**
	 * @param first the first to set
	 */
	public void setFirst(Text first) {
		this.first = first;
	}

	/**
	 * @return the second
	 */
	public Text getSecond() {
		return second;
	}

	/**
	 * @param second the second to set
	 */
	public void setSecond(Text second) {
		this.second = second;
	}

	/**
	 * Default constructor
	 */
	public KeyPair() {
		this.first = new Text();
		this.second = new Text();
	}
	
	/**
	 * Overloaded constructor
	 * @param value
	 * @param type
	 */
	public KeyPair(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	/**
	 * Update the value for value and type
	 * @param first
	 * @param second 
	 */
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	public void readFields(DataInput in) throws IOException {
		this.first.readFields(in);
		this.second.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.first.write(out);
		this.second.write(out);
	}

	public int compareTo(KeyPair pair) {
		int cmp = this.first.compareTo(pair.getFirst());
		
		if (cmp != 0) {
			return cmp;
		}
		
		return this.second.compareTo(pair.getSecond());
	}
	
	public int hashCode() {
		return this.first.hashCode() * 163 + this.first.hashCode();
	}	
	
	public boolean equals(Object o) {
		if (o instanceof KeyPair) {
			KeyPair arg = (KeyPair) o;
			return (this.first.equals(arg.getFirst())
					&& this.second.equals(arg.getSecond()));
		}
		return false;
	}

}
