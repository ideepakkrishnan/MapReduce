/**
 * 
 */
package com.neu.pdp.resources;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Stores the execution timer readings 
 * @author ideepakkrishnan
 */
public class Result {
	
	private static final Logger logger = LogManager.getLogger(
			Result.class.getName());
	
	private Long minExecutionTime;
	private Long maxExecutionTime;
	private Long avgExecutionTime;
	private List<Long> executionTimes;
	
	public Result() {
		minExecutionTime = null;
		maxExecutionTime = null;
		avgExecutionTime = null;
		executionTimes = new ArrayList<Long>();
	}
	
	// Exposed methods
	
	/**
	 * Adds an execution time to the local list object
	 * @param executionTime Execution time in milliseconds
	 */
	public void addExecutionTime(Long executionTime) {
		executionTimes.add(executionTime);
	}
	
	// Getters and Setters

	/**
	 * @return the minExecutionTime
	 */
	public Long getMinExecutionTime() {
		return minExecutionTime;
	}

	/**
	 * @param minExecutionTime the minExecutionTime to set
	 */
	public void setMinExecutionTime(Long minExecutionTime) {
		this.minExecutionTime = minExecutionTime;
	}

	/**
	 * @return the maxExecutionTime
	 */
	public Long getMaxExecutionTime() {
		return maxExecutionTime;
	}

	/**
	 * @param maxExecutionTime the maxExecutionTime to set
	 */
	public void setMaxExecutionTime(Long maxExecutionTime) {
		this.maxExecutionTime = maxExecutionTime;
	}

	/**
	 * @return the avgExecutionTime
	 */
	public Long getAvgExecutionTime() {
		return avgExecutionTime;
	}

	/**
	 * @param avgExecutionTime the avgExecutionTime to set
	 */
	public void setAvgExecutionTime(Long avgExecutionTime) {
		this.avgExecutionTime = avgExecutionTime;
	}

	/**
	 * @return the executionTimes
	 */
	public List<Long> getExecutionTimes() {
		return executionTimes;
	}

	/**
	 * @param executionTimes the executionTimes to set
	 */
	public void setExecutionTimes(List<Long> executionTimes) {
		this.executionTimes = executionTimes;
	}
	
	public void updateExecutionTimes() {
		logger.info("Entering updateExecutionTimes method");		
		
		// Set the minimum execution time
		setMinExecutionTime(
				Util.findMinimumExecutionTime(
						executionTimes));
		
		// Set the maximum execution time
		setMaxExecutionTime(
				Util.findMaximumExecutionTime(
						executionTimes));
		
		// Set the average execution time
		setAvgExecutionTime(
				Util.findAverageExecutionTime(
						executionTimes));
		
		logger.info("Returning from updateExecutionTimes method");
	}
}
