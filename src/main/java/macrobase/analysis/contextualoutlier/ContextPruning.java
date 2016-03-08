package macrobase.analysis.contextualoutlier;

import java.util.*;

import org.apache.commons.math3.distribution.NormalDistribution;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.datamodel.Datum;

public class ContextPruning {

	public static List<Datum> data;	//the entire data
	public static List<Datum> sampleData; // a random sample from the entire data
	public static double alpha = 0.05;
	
	public static OutlierDetector detector;
	
	//These pruning can entirely prune a context and all sub-contexts
	public static int numDensityPruning = 0;
	public static int numDependencyPruning = 0;
	
	
	//These pruning can only prune the current context
	public static int numPdfPruning = 0;
	public static int numDetectorSpecificPruning = 0;
	
	
	/**
	 * Estimating the size of a context
	 * The Null Hypothesis is that density(c) >= minDensity
	 * @param c
	 * @param minDensity
	 * @return true if the estimation > minSize
	 */
	public static boolean densityPruning(Context c, double minDensity){
		
		int sampleSize = sampleData.size();
		int sampleHit = 0;
		for(Datum d : sampleData){
			if(c.containDatum(d)){
				sampleHit++;
			}
		}
		
		if(sampleHit < 10 || (sampleSize - sampleHit) < 10){
			//too small to use the statistical test
			return false;
		}
		
		double estimatedDensity = (double) sampleHit / sampleSize;
		
		double sampleSD = Math.sqrt(minDensity * (1-minDensity) / sampleSize);
		double zScore = (estimatedDensity - minDensity) / sampleSD;
		
		NormalDistribution unitNormal = new NormalDistribution(0d, 1d);
		double pValue = unitNormal.cumulativeProbability(zScore);
		
		
		if(pValue <= alpha){
			//reject the hypothesis, thus can be pruned
			return true;
		}else{
			//fail to reject
			return false;
		}
		
	}
	
	/**
	 * For two parents p1, and p2, if p1=>p2, or p2=>p1, then c should not be generated
	 * using a sample of p1, and a sample of p2
	 * This is not mean testing, a point 
	 * @param c
	 * @return
	 */
	public static boolean dependencyPruning(Context c){
		Context p1 = c.getParents().get(0);
		Context p2 = c.getParents().get(1);
		
		boolean sample_p1_p2 = true;
		for(Datum d: p1.getSample()){
			if(!p2.containDatum(d)){
				sample_p1_p2 = false;
			}
		}
		
		boolean sample_p2_p1 = true;
		for(Datum d: p2.getSample()){
			if(!p1.containDatum(d)){
				sample_p2_p1 = false;
			}
		}
		
		
		if(sample_p1_p2 && dependencyTest(p1,p2)){
			numDependencyPruning++;
			return true;
		}else if(sample_p2_p1 && dependencyTest(p2,p1)){
			numDependencyPruning++;
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * All tuples in p1 are in p2 as well
	 * @param p1
	 * @param p2
	 * @return
	 */
	private static boolean dependencyTest(Context p1, Context p2){
		for(Datum d: data){
			if(p1.containDatum(d)){
				if(!p2.containDatum(d))
					return false;
			}
		}
		return true;
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
