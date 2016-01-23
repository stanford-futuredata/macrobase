package macrobase.runtime.standalone.scoping;


/**
 * @author xuchu
 *
 * This class represents the interval on one dimension
 */


public class Interval {

	private int dimension;
	
	private double min;
	private double max; 
	
	private int value;
	
	/**
	 * Create a numerical interval
	 * @param dimension
	 * @param min
	 * @param max
	 */
	public Interval(int dimension, double min, double max) {
	    this.setDimension(dimension);
	    this.setMin(min);
	    this.setMax(max);
	}
	
	/**
	 * Create a categorical interval
	 * @param dimension
	 * @param value
	 */
	public Interval(int dimension, int value){
		this.setDimension(dimension);
		this.setValue(value);
	}

	public int getDimension() {
		return dimension;
	}

	public void setDimension(int dimension) {
		this.dimension = dimension;
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	/**
	 * If the interval contains this double value
	 * This function should only be invoked for numerical intervals
	 * @param d
	 * @return
	 */
	public boolean contains(double d){
		if(d >= min && d < max){
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * If the interval contains this integer value
	 * This function should only be invoked for categorical intervals
	 * @param i
	 * @return
	 */
	public boolean contains(int i){
		if(value == i)
			return true;
		else
			return false;
	}
	
}
