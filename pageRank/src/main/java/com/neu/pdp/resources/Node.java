/**
 * 
 */
package com.neu.pdp.resources;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author ideepakkrishnan
 *
 */
public class Node implements Writable {
	
	// Class level attributes
	private DoubleWritable pageRank;
	private TextArrayWritable adjacencyList;
	
	/**
	 * Default Constructor
	 */
	public Node() {
		this.pageRank = new DoubleWritable();
		this.adjacencyList = new TextArrayWritable(
				new Text[0]);
	}
	
	/**
	 * Overloaded Constructor
	 * @param pageRank
	 * @param adjacencyList
	 */
	public Node(
			DoubleWritable pageRank, 
			TextArrayWritable adjacencyList) {
		this.pageRank = pageRank;
		this.adjacencyList = adjacencyList;
	}	

	/**
	 * @return the pageRank
	 */
	public DoubleWritable getPageRank() {
		return pageRank;
	}

	/**
	 * @param pageRank the pageRank to set
	 */
	public void setPageRank(DoubleWritable pageRank) {
		this.pageRank = pageRank;
	}

	/**
	 * @return the adjacencyList
	 */
	public TextArrayWritable getAdjacencyList() {
		return adjacencyList;
	}

	/**
	 * @param adjacencyList the adjacencyList to set
	 */
	public void setAdjacencyList(TextArrayWritable adjacencyList) {
		this.adjacencyList = adjacencyList;
	}
	
	public void set( 
			DoubleWritable pageRank, 
			TextArrayWritable adjacencyList) {
		this.pageRank = pageRank;
		this.adjacencyList = adjacencyList;
	}

	public void readFields(DataInput in) throws IOException {
		this.pageRank.readFields(in);
		this.adjacencyList.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.pageRank.write(out);
		this.adjacencyList.write(out);
	}

}
