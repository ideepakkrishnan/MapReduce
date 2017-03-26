/**
 * 
 */
package com.neu.pdp.pageRank.resources;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * @author ideepakkrishnan
 *
 */
public class TextArrayWritable extends ArrayWritable {

	public TextArrayWritable(Text[] values) {
		super(Text.class, values);
	}
		
    public Text[] get() {
        return (Text[]) super.get();
    }
	
	public void set(Text[] values) {
		super.set(values);
	}

    @Override
    public String toString() {
    	Text[] values = get();
        return values[0].toString() + ", " + values[1].toString();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	//Text[] values = new Writable(in.read);
    	try {
    		super.readFields(in);
    	} catch (EOFException e) {
    		// Skip
    	}
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
    	Text[] values = get();
    	if (values != null) {
	    	for (int i = 0; i < values.length; i++) {
	    		if (values[i] != null) {
	    			values[i].write(out);
	    		}
	    	}
    	}
    }

}
