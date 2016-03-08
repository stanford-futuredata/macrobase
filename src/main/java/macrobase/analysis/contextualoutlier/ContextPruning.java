package macrobase.analysis.contextualoutlier;

import java.util.*;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.datamodel.Datum;

public class ContextPruning {


	public static List<Datum> sampleData; // a random sample from the entire data
	public static double errorBound;
	
	public static OutlierDetector detector;
	
	//These pruning can entirely prune a context and all sub-contexts
	public static int numDensityPruning = 0;
	public static int numDependencyPruning = 0;
	
	
	//These pruning can only prune the current context
	public static int numPdfPruning = 0;
	public static int numDetectorSpecificPruning = 0;
	
	
	/**
	 * Estimating the size of a context
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
	
	/**
	 * For two parents p1, and p2, if p1=>p2, or p2=>p1, then c should not be generated
	 * @param c
	 * @return
	 */
	public static boolean dependencyPruning(Context c){
		Context p1 = c.getParents().get(0);
		Context p2 = c.getParents().get(1);
		
		boolean p1_p2 = true;
		for(Datum d: p1.getSample()){
			if(!p2.containDatum(d)){
				p1_p2 = false;
			}
		}
		
		boolean p2_p1 = true;
		for(Datum d: p2.getSample()){
			if(!p1.containDatum(d)){
				p2_p1 = false;
			}
		}
		
		
		if(p1_p2 || p2_p1){
			numDependencyPruning++;
			return true;
		}else{
			return false;
		}
	}
	
	
	/**
	 * Determine if the context can be pruned with running f
	 * If the pdf of p1 is the same(similar) to the pdf of p2
	 * @param c
	 * @return
	 */
	public static boolean pdfPruning(Context c){
		return false;
	}
	
	/**
	 * Determine if the context can be pruned without running f, 
	 * but can use the internals of f
	 * @param c
	 * @return
	 */
	public static boolean detectorSpecificPruning(Context c){
		return false;
	}
	

	
	public static String print(){
		StringBuilder sb = new StringBuilder();
		sb.append("numDensityPruning: " + numDensityPruning + "  ");
		sb.append("numDependencyPruning: " + numDependencyPruning + "  ");
		sb.append("numPdfPruning: " + numPdfPruning + "  ");
		sb.append("numDetectorSpecificPruning: " + numDetectorSpecificPruning + "   ");
		return sb.toString();
	}
	
}
