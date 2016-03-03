package macrobase.analysis.contextualoutlier;

import java.util.*;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.datamodel.Datum;

public class ContextPruning {

	public static List<Datum> data;

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
	 * @param c1
	 * @param c2
	 * @param minSize
	 * @return true if the estimation > minSize
	 */
	public static boolean densityPruning(Context c1, Context c2, int minSize){
		
		return false;
	}
	
	
	private static Set<Integer> randomSample(List<Integer> items, int m){
		Random rnd = new Random();
	    HashSet<Integer> res = new HashSet<Integer>(m);
	    int n = items.size();
	    for(int i=n-m;i<n;i++){
	        int pos = rnd.nextInt(i+1);
	        Integer item = items.get(pos);
	        if (res.contains(item))
	            res.add(items.get(i));
	        else
	            res.add(item);
	    }
	    return res;
	}
}
