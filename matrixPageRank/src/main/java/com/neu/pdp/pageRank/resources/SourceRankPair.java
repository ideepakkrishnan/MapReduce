/**
 * 
 */
package com.neu.pdp.pageRank.resources;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author ideepakkrishnan
 *
 */
public class SourceRankPair implements WritableComparable<SourceRankPair> {
	
	private Text dest;
	private LongWritable source;
	private DoubleWritable rank;

	/**
	 * @return the dest
	 */
	public Text getDest() {
		return dest;
	}

	/**
	 * @param dest the dest to set
	 */
	public void setDest(Text dest) {
		this.dest = dest;
	}

	/**
	 * @return the source
	 */
	public LongWritable getSource() {
		return source;
	}

	/**
	 * @param source the source to set
	 */
	public void setSource(LongWritable source) {
		this.source = source;
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
	
	public SourceRankPair() {
		this.dest = new Text();
		this.source = new LongWritable();
		this.rank = new DoubleWritable();
	}
	
	public SourceRankPair(Text dest, LongWritable source, DoubleWritable rank) {
		this.dest = dest;
		this.source = source;
		this.rank = rank;
	}
	
	public void set(Text dest, LongWritable source, DoubleWritable rank) {
		this.dest = dest;
		this.source = source;
		this.rank = rank;
	}

	public void write(DataOutput out) throws IOException {
		this.dest.write(out);
		out.writeLong(this.source.get());
		out.writeDouble(this.rank.get());
	}

	public void readFields(DataInput in) throws IOException {
		this.dest.readFields(in);
		this.source.readFields(in);
		this.rank.readFields(in);
	}

	public int compareTo(SourceRankPair o) {
		int cmp = this.dest.compareTo(o.getDest());
		
		if (cmp != 0) {
			return cmp;
		}
		
		return this.source.compareTo(o.getSource());
	}
	
	public int hashCode() {
		return this.dest.hashCode() * 163 
				+ this.source.hashCode() * 163 
				+ this.rank.hashCode();
	}	
	
	public boolean equals(Object o) {
		if (o instanceof SourceRankPair) {
			SourceRankPair arg = (SourceRankPair) o;
			return (this.dest.equals(arg.getDest())
					&& this.source.equals(arg.getSource())
					&& this.rank.equals(arg.getRank()));
		}
		
		return false;
	}

}
