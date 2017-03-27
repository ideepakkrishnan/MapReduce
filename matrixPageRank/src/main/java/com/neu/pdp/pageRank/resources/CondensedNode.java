/**
 * 
 */
package com.neu.pdp.pageRank.resources;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author ideepakkrishnan
 *
 */
public class CondensedNode implements WritableComparable<CondensedNode> {

	// Class level attributes
	private Text name;
	private DoubleWritable rank;
	
	/**
	 * Default Constructor
	 */
	public CondensedNode() {
		this.name = new Text();
		this.rank = new DoubleWritable();
	}
	
	/**
	 * Overloaded Constructor
	 * @param name
	 * @param rank
	 */
	public CondensedNode(
			Text name,
			DoubleWritable rank) {
		this.name = name;
		this.rank = rank;
	}
	
	/**
	 * @return the name
	 */
	public Text getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(Text name) {
		this.name = name;
	}

	/**
	 * @return the rank
	 */
	public DoubleWritable getRank() {
		return rank;
	}

	/**
	 * @param rank the rank to set
	 */
	public void setRank(DoubleWritable rank) {
		this.rank = rank;
	}
	
	public void set(
			Text name,
			DoubleWritable rank) {
		this.name = name;
		this.rank = rank;
	}

	public void readFields(DataInput in) throws IOException {
		this.name.readFields(in);
		this.rank.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.name.write(out);
		this.rank.write(out);
	}
	
	@Override
	public boolean equals(Object obj) {
	    if (obj == null) {
	        return false;
	    }
	    
	    if (!CondensedNode.class.isAssignableFrom(
	    		obj.getClass())) {
	        return false;
	    }
	    
	    CondensedNode other = (CondensedNode) obj;
	    if ((this.name == null) 
	    		? (other.name != null) : 
	    			!this.name.equals(other.name)) {
	        return false;
	    }
	    
	    if (this.rank != other.rank) {
	        return false;
	    }
	    
	    return true;
	}
	
	@Override
	public String toString() {		
		return "{" + this.name.toString() + 
				" : " + this.rank.toString() + 
				"} @ " + this.hashCode();
	}

	public int compareTo(CondensedNode o) {
		int cmp = this.name.compareTo(o.getName());
		
		if (cmp != 0) {
			return cmp;
		}
		
		return this.rank.compareTo(o.getRank());
	}

}
