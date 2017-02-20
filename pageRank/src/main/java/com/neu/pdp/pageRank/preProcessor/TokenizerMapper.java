/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Pre-processing mapper.
 * This class extracts external links from a page and passes
 * it on to the reducer.
 * @author ideepakkrishnan
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * Reads data from a compressed bz2 file and extracts
	 * the href values for anchor tags inside the div tag
	 * with id 'bodyContent'. These extracted links are
	 * passed on to the reducer as key-value pairs where
	 * key represents the current page and value represents
	 * an external link from that page.
	 * @param key The input key
	 * @param value The input value as Text
	 * @param context The current context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
		while (itr.hasMoreTokens()) {
			System.out.println(itr.nextToken());
		}
	}
}
