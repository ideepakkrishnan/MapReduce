/**
 * 
 */
package com.neu.pdp.resources;

import java.io.DataInput;
import java.io.DataOutput;
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
    	super.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {      
      super.write(out);
    }

}
