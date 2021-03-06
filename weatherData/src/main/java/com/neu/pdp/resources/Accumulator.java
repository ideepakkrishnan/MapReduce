/**
 * 
 */
package com.neu.pdp.resources;

/**
 * @author ideepakkrishnan
 *
 */
public class Accumulator {
	
	private int sum;
	private int count;
	
	public Accumulator() {
		this.sum = 0;
		this.count = 0;
	}
	
	public Accumulator(int value) {
		this.sum = value;
		this.count = 1;
	}
		
	/**
	 * @return the sum
	 */
	public int getSum() {
		return sum;
	}

	/**
	 * @param sum the sum to set
	 */
	public void setSum(int sum) {
		this.sum = sum;
	}

	/**
	 * @return the count
	 */
	public int getCount() {
		return count;
	}

	/**
	 * @param count the count to set
	 */
	public void setCount(int count) {
		this.count = count;
	}

	/**
	 * Adds a new value to the sum and increases the
	 * count by 1
	 * @param value
	 */
	public void addValue(int value, boolean addFibonacci) {
		this.sum += value;
		this.count += 1;
		
		if (addFibonacci) {
			Util.fibonacci(17);
		}
	}
	
	/**
	 * Adds a new value to the running sum synchronously 
	 * @param value
	 * @param addFibonacci
	 */
	public synchronized void addValueSynchronously(
			int value, boolean addFibonacci) {
		this.sum += value;
		this.count += 1;
		
		if (addFibonacci) {
			Util.fibonacci(17);
		}
	}
	
	/**
	 * Adds the values passed in as arguments to the
	 * local sum and count fields
	 * @param sum The value to be added to sum
	 * @param count The value to be added to count
	 */
	public void combineValues(int sum, int count) {
		this.sum += sum;
		this.count += count;
	}

}
