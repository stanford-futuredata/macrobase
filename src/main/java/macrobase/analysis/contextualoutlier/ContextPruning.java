package macrobase.analysis.contextualoutlier;

import java.util.*;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.inference.TestUtils;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.datamodel.Datum;

public class ContextPruning {

	public static ContextPruningOptions contextPruningOptions;
	
	public static List<Datum> data;	//the entire data
	public static HashSet<Datum> sample; // a random sample for the globalContext
	public static double alpha = 0.05;
	
	
	//These pruning can entirely prune a context and all sub-contexts
	public static int numDensityPruning = 0;
	public static int numDependencyPruning = 0;
	
	
	//These pruning can only prune the current context
	public static int numDetectorSpecificPruning = 0;
	
	
	
	/**
	 * Estimating the size of a context
	 * The Null Hypothesis is that density(c) >= minDensity
	 * @param c
	 * @param minDensity
	 * @return true if the estimation > minSize
	 */
	public static boolean densityPruning(Context c, double minDensity){
		
		if(contextPruningOptions.isDensityPruning() == false)
			return false;
		
		int sampleSize = sample.size();
		int sampleHit = c.getSample().size();
		
		
		return densityPruning(sampleSize, sampleHit, minDensity);
		
	}
	
	/**
	 * Estimating the size of a context
	 * The Null Hypothesis is that (sampleHit/sampleSize) >= minDensity
	 * @param c
	 * @param minDensity
	 * @return true if the estimation > minSize
	 */
	public static boolean densityPruning(int sampleSize, int sampleHit, double minDensity){
		double estimatedDensity = (double) sampleHit / sampleSize;
		
		double sampleSD = Math.sqrt(minDensity * (1-minDensity) / sampleSize);
		double zScore = (estimatedDensity - minDensity) / sampleSD;
		
		NormalDistribution unitNormal = new NormalDistribution(0d, 1d);
		double pValue = unitNormal.cumulativeProbability(zScore);
		
		
		if(pValue <= alpha){
			//reject the hypothesis, thus can be pruned
			numDensityPruning++;
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
		
		if(contextPruningOptions.isDependencyPruning() == false)
			return false;
			
		Context p1 = c.getParents().get(0);
		Context p2 = c.getParents().get(1);
		
		boolean sample_p1_p2 = true;
		for(Datum d: p1.getSample()){
			if(!p2.containDatum(d)){
				sample_p1_p2 = false;
				break;
			}
		}
		
		boolean sample_p2_p1 = true;
		for(Datum d: p2.getSample()){
			if(!p1.containDatum(d)){
				sample_p2_p1 = false;
				break;
			}
		}
		
		
		if(sample_p1_p2 ){
			numDependencyPruning++;
			return true;
//			if(dependencyTest(p1,p2)){
//				numDependencyPruning++;
//				return true;
//			}else{
//				falseDependencyUsingSample++;
//				return false;
//			}
			
		}else if(sample_p2_p1){
			numDependencyPruning++;
			return true;
//			if(dependencyTest(p2,p1)){
//				numDependencyPruning++;
//				return true;
//			}else{
//				falseDependencyUsingSample++;
//				return false;
//			}
			
		}else{
			return false;
		}
	}
	
	public static int falseDependencyUsingSample = 0;
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
	
	
	public static int numSameDistributions = 0;
	/**
	 * Determine if two contexts have same distributions
	 * @param p1
	 * @param p2
	 * @return
	 */
	public static boolean sameDistribution(Context p1, Context p2){
		
		if(contextPruningOptions.isDistributionPruningForTraining() == false 
				&& contextPruningOptions.isDistributionPruningForScoring() == false){
			return false;
		}
		
		HashSet<Datum> sample1 = p1.getSample();
		HashSet<Datum> sample2 = p2.getSample();
		
		double[] values1 = new double[sample1.size()];
		int i = 0;
		for(Datum d: sample1){
			values1[i] = d.getMetrics().getEntry(0);
			i++;
		}
			
		double[] values2 = new double[sample2.size()];
		int j = 0; 
		for(Datum d: sample2){
			values2[j] = d.getMetrics().getEntry(0);
			j++;
		}
		
		double pValue = TestUtils.kolmogorovSmirnovTest(values1, values2);
		
		boolean rejectNull = (pValue <= alpha)? true: false;	
		
		if(rejectNull){
			//reject null, leads to different distribution
			return false;
		}else{
			//accept null, which is same distribution
			numSameDistributions++;
			return true;
		}
		
	}
	

	
	public static String print(){
		StringBuilder sb = new StringBuilder();
		sb.append("numDensityPruning: " + numDensityPruning + "  ");
		sb.append("numDependencyPruning: " + numDependencyPruning + "  " + " falseDependencyUsingSample:  " + falseDependencyUsingSample + "  ");
		sb.append("numSameDistributions: " + numSameDistributions + "  ");
		sb.append("numDetectorSpecificPruning: " + numDetectorSpecificPruning + "   ");
		return sb.toString();
	}
	
}
