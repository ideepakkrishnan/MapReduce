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
 * for secondary sort. It implements WritableComparable so
 * that the compare method could be used in KeyComparator and
 * GroupComparator for processing.
 * @author ideepakkrishnan
 *
 */
public class KeyPair implements WritableComparable<KeyPair> {
	
	private Text stationId;
	private IntWritable year;
	
	/**
	 * Default constructor
	 */
	public KeyPair() {
		this.stationId = new Text();
		this.year = new IntWritable();
	}
	
	/**
	 * Overloaded constructor
	 * @param stationId
	 * @param year
	 */
	public KeyPair(Text stationId, IntWritable year) {
		this.stationId = stationId;
		this.year = year;
	}

	/**
	 * @return the stationId
	 */
	public Text getStationId() {
		return stationId;
	}

	/**
	 * @param stationId the stationId to set
	 */
	public void setStationId(Text stationId) {
		this.stationId = stationId;
	}

	/**
	 * @return the year
	 */
	public IntWritable getYear() {
		return year;
	}

	/**
	 * @param year the year to set
	 */
	public void setYear(IntWritable year) {
		this.year = year;
	}
	
	/**
	 * Update the value for station ID and year
	 * @param stationId Station ID as a String
	 * @param year Year as an integer
	 */
	public void set(Text stationId, IntWritable year) {
		this.stationId = stationId;
		this.year = year;
	}

	public void readFields(DataInput in) throws IOException {
		this.stationId.readFields(in);
		this.year.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.stationId.write(out);
		this.year.write(out);
	}

	public int compareTo(KeyPair pair) {
		int cmp = this.stationId.compareTo(pair.getStationId());
		
		if (cmp != 0) {
			return cmp;
		}
		
		return this.year.compareTo(pair.getYear());
	}
	
	public int hashCode() {
		return this.stationId.hashCode() * 163 + this.year.hashCode();
	}	
	
	public boolean equals(Object o) {
		if (o instanceof KeyPair) {
			KeyPair arg = (KeyPair) o;
			return (this.stationId.equals(arg.getStationId())
					&& this.year.equals(arg.getYear()));
		}
		return false;
	}

}
