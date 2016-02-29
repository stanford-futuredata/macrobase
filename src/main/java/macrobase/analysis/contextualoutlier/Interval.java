package macrobase.analysis.contextualoutlier;

import macrobase.ingest.DatumEncoder;

/**
 * @author xuchu
 *
 * This class represents the interval on one dimension
 */


public abstract class Interval {

	protected int dimension;
	protected String columnName;
	

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public int getDimension() {
		return dimension;
	}

	public void setDimension(int dimension) {
		this.dimension = dimension;
	}

	

	public abstract boolean contains(Object d);
	
	
	/**
	 * Create a interval
	 * @param dimension
	 * @param min
	 * @param max
	 */
	public Interval(int dimension, String columnName) {
	    this.setDimension(dimension);
	    this.setColumnName(columnName);
	   
	}
	
	/**
	 * Provide a human-readable print of the Interval
	 * @param encoder
	 * @return
	 */
	public abstract String print(DatumEncoder encoder);
}