package macrobase.analysis.contextualoutlier;

import java.util.*;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.datamodel.Datum;

public class ContextPruning {

	public static List<Datum> data;

	public static List<Datum> sampleData;
	public static double errorBound;
	
	public static OutlierDetector detector;
	
	public static int numDensityPruning = 0;
	public static int numpdfPruning = 0;
	public static int numDetectorSpecificPruning = 0;
	
	/**
	 * Determine if the conjunction of two contexts have outliers or not
	 * without actually running detector
	 * @param c1
	 * @param c2
	 * @return
	 */
	public static boolean pdfPruning(Context c1, Context c2){
		return false;
	}
	
	/**
	 * Determine if the conjunction of two contexts have outliers or not
	 * without actually running detector
	 * @param c1
	 * @param c2
	 * @return
	 */
	public static boolean detectorSpecificPruning(Context c1, Context c2){
		return false;
	}
	
	/**
	 * Estimating the size of the intersection of two contexts
	 * @param c
	 * @param minDensity
	 * @return true if the estimation > minSize
	 */
	public static boolean densityPruning(Context c, double minDensity){
		
		int sampleContextCount = 0;
		for(Datum d : sampleData){
			if(c.containDatum(d)){
				sampleContextCount++;
			}
		}
		
		double estimatedDensity = (double) sampleContextCount / sampleData.size();
		
		//estimation + variance
		double maxBound = estimatedDensity + errorBound;
		
		if(maxBound < minDensity){
			numDensityPruning++;
			return true;
		}else{
			return false;
		}
		
	}
	
	
}
