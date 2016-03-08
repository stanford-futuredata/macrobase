package macrobase.analysis.contextualoutlier;

import static org.junit.Assert.*;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.inference.AlternativeHypothesis;
import org.apache.commons.math3.stat.inference.BinomialTest;
import org.apache.commons.math3.stat.inference.TestUtils;
import org.junit.Test;

public class HypothesisTestingTest {

	//significance level alpha is the probability of rejecting the null hypothesis given that it is true
	//The null hypothesis is rejected is p-value <= alpha
	double alpha = .05;

	
	
	public HypothesisTestingTest() {
		// TODO Auto-generated constructor stub
	}

	@Test
	public void tTest(){
		double[] observed = {1d, 2d, 3d};
		double mu = 10d;
		
		double pValue = TestUtils.tTest(mu, observed);
		
		boolean rejectNull = (pValue <= alpha)? true: false;
		assertEquals(rejectNull, true);
	}
	
	/**
	 * It seems like ksTest can tolerate outliers 
	 */
	@Test
	public void ksTest(){
		
		double[] observed = {-0.1d, 0d, 0.1d};
		final NormalDistribution unitNormal = new NormalDistribution(0d, 1d);
		boolean rejectNullHypo = TestUtils.kolmogorovSmirnovTest(unitNormal, observed,alpha);
		
		//fail to reject, i.e., observed indeed come from normal distribution
		assertEquals(rejectNullHypo, false);
		
		
		double[] data1 = {1.0, 2.0, 3.0};
		double[] data2 = {1.0, 2.0, 3.0};
		
		double pValue = TestUtils.kolmogorovSmirnovTest(data1,data2);
		boolean rejectNull = (pValue <= alpha)? true: false;		
		assertEquals(rejectNull, false);
		
		
		double[] data3 = {1.0, 2.0, 3.0};
		double[] data4 = {10, 20, 30};
		pValue = TestUtils.kolmogorovSmirnovTest(data3,data4);
		rejectNull = (pValue <= alpha)? true: false;		
		assertEquals(rejectNull, true);

		
	}
	
	/**
	 * It seems that ks-test is tolerate to outliers
	 */
	@Test
	public void ksTestToleranceToOutliers(){
		
		double[] observed = {-0.1d, 0d, 0.1d};
		final NormalDistribution unitNormal = new NormalDistribution(0d, 1d);
		boolean rejectNullHypo = TestUtils.kolmogorovSmirnovTest(unitNormal, observed,alpha);
		
		//fail to reject, i.e., observed indeed come from normal distribution
		assertEquals(rejectNullHypo, false);
		
		double[] data1 = new double[100];
		for(int i = 0; i < data1.length; i++){
			data1[i] = i;
		}
		
		double[] data2 = new double[50];
		for(int i = 0; i < data2.length; i++){
			data2[i] = i * 2;
		}
		data2[46] = 999;
		data2[47] = 1000;
		data2[48] = 999;
		data2[49] = 1000;
		
		
		double pValue = TestUtils.kolmogorovSmirnovTest(data1,data2);
		
		boolean rejectNull = (pValue <= alpha)? true: false;		
		assertEquals(rejectNull, false);
		
	}   
	
	@Test
	public void propotionTest(){
		
		int sampleSize = 20;
		int sampleHit = 19;
		double minDensity = 0.9;
		
		boolean pruned = false;
		
		
		double estimatedDensity = (double) sampleHit / sampleSize;
		
		double sampleSD = Math.sqrt(minDensity * (1-minDensity) / sampleSize);
		double zScore = (estimatedDensity - minDensity) / sampleSD;
		
		NormalDistribution unitNormal = new NormalDistribution(0d, 1d);
		double pValue = unitNormal.cumulativeProbability(zScore);
		
		
		if(pValue <= alpha){
			//reject the hypothesis, thus can be pruned
			pruned = true;
		}else{
			//fail to reject
			pruned = false;
		}
		
		
		assertEquals(pruned,false);
	}
	
}
