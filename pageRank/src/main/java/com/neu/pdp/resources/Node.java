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
	private Text type;
	private Text adjacencyList;
	
	/**
	 * Default Constructor
	 */
	public Node() {
		this.pageRank = new DoubleWritable();
		this.setType(new Text());
		this.adjacencyList = new Text();
	}
	
	/**
	 * Overloaded Constructor
	 * @param pageRank
	 * @param adjacencyList
	 */
	public Node(
			DoubleWritable pageRank,
			Text type,
			Text adjacencyList) {
		this.pageRank = pageRank;
		this.setType(type);
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
	 * @return the type
	 */
	public Text getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(Text type) {
		this.type = type;
	}

	/**
	 * @return the adjacencyList
	 */
	public Text getAdjacencyList() {
		return adjacencyList;
	}

	/**
	 * @param adjacencyList the adjacencyList to set
	 */
	public void setAdjacencyList(Text adjacencyList) {
		this.adjacencyList = adjacencyList;
	}
	
	public void set( 
			DoubleWritable pageRank, 
			Text type,
			Text adjacencyList) {
		this.pageRank = pageRank;
		this.type = type;
		this.adjacencyList = adjacencyList;
	}

	public void readFields(DataInput in) throws IOException {
		this.pageRank.readFields(in);
		this.type.readFields(in);
		this.adjacencyList.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.pageRank.write(out);
		this.type.write(out);
		this.adjacencyList.write(out);
	}

}
