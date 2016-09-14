package macrobase.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.distribution.*;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.inference.TestUtils;


public class HypothesisTesting {

    public static boolean sameProportionTest(int sampleSize, double samplePercentage, double realPercentage, double significance_level) {
        
        double stardardDeviation = Math.sqrt(realPercentage * (1 - realPercentage) / sampleSize);
        double zScore = (samplePercentage - realPercentage) / stardardDeviation;
        
        NormalDistribution nd = new NormalDistribution();
        double pValue = 2 * nd.cumulativeProbability(-Math.abs(zScore));
        
        if (pValue <= significance_level) {
            return false;
        } else {
            return true;
        }
    }
    
    
    
    /**
     * Test if signs array has equal number of +'s and -'s
     * @param signs
     * @return
     */
    public static boolean samePosNeg(int numPos, int numNeg, double significance_level) {
       
        BinomialDistribution bd = new BinomialDistribution((numPos + numNeg), 0.5);
        int min = Math.min(numPos, numNeg);
        double pValue = 2 * (bd.cumulativeProbability(min));

        if (pValue <= significance_level) {
            //reject the hypothesis
            return false;
        } else {
            return true;
        }
    }
    
    public static boolean independenceTest(Integer[] v1, Integer[] v2, double significance_level) {
        int d1 = 0;
        Map<Integer, Integer> value2Index1 = new HashMap<Integer, Integer>();
        for (Integer value: v1) {
            if (!value2Index1.containsKey(value)) {
                value2Index1.put(value, d1);
                d1++;
            }
        }
        
        int d2 = 0;
        Map<Integer, Integer> value2Index2 = new HashMap<Integer, Integer>();
        for (Integer value: v2) {
            if (!value2Index2.containsKey(value)) {
                value2Index2.put(value, d2);
                d2++;
            }
        }
        assert(v1.length == v2.length);
        
        if (value2Index1.keySet().size() == 1 || value2Index2.keySet().size() == 1) {
            System.err.println("cannot perform chi test");
            return false;
        }
        
        long[][] counts = new long[d1][d2];
        for (int i = 0; i < v1.length; i++) {
            Integer value1 = v1[i];
            Integer value2 = v2[i];
            int index1 = value2Index1.get(value1);
            int index2 = value2Index2.get(value2);
            counts[index1][index2]++;
        }
        //return true iff null hypothesis can be rejected with confidence 1 - alpha
        double pValue = TestUtils.chiSquareTest(counts);
        if( pValue <= significance_level ){
            //reject the null hypothesis that they are independent
            return false;
        } else {
            return true;
        }
    }
    public static boolean independenceTest(long[][] counts, double significance_level) {
        double pValue = TestUtils.chiSquareTest(counts);
        if( pValue <= significance_level ){
            //reject the null hypothesis that they are independent
            return false;
        } else {
            return true;
        }
    }
    
    
    public static boolean sameMean(double[] sample1, double[] sample2, double significance_level) {
        
        double pValue = TestUtils.tTest(sample1, sample2);
       
        /*
        double sampleMean1 = StatUtils.mean(sample1);
        double sampleVariance1 = StatUtils.variance(sample1);

        double sampleMean2 = StatUtils.mean(sample2);
        double sampleVariance2 = StatUtils.variance(sample2);
        
        double testStatistic = (sampleMean1 - sampleMean2) / Math.sqrt(sampleVariance1/sample1.length + sampleVariance2/ sample2.length);
        
        double degreeOfFreedom = Math.pow( sampleVariance1/sample1.length + sampleVariance2/sample2.length, 2) / 
                ( Math.pow(sampleVariance1/sample1.length, 2)/(sample1.length - 1) + Math.pow(sampleVariance2/sample2.length, 2)/(sample2.length - 1));
        
        TDistribution tDistribution = new TDistribution(degreeOfFreedom);
        
        double pValue = 0;
        if (testStatistic >= 0) {
            double cd =  tDistribution.cumulativeProbability(-testStatistic);
            //t-distribution is symmetric and this is two tailed test
            pValue = 2 * cd;
        } else {
            double cd =  tDistribution.cumulativeProbability(testStatistic);
            pValue = 2 * cd;
        }
        */
        //double pValueFromPackage = TestUtils.tTest(sample1, sample2);
        
        if (pValue <= significance_level) {
            //reject the null hypothesis that two means are the same, i.e., different mean
            return false;
        } else {
            return true;
        }
        
    }
    
    /**
     * http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/stat/inference/KolmogorovSmirnovTest.html#kolmogorovSmirnovStatistic(double[],%20double[])
     * http://www.itl.nist.gov/div898/handbook/eda/section3/eda35g.htm
     * @param sample1
     * @param sample2
     * @param significance_level
     * @return
     */
    public static boolean sameDistributionKS(double[] sample1, double[] sample2, double significance_level) {
        double pValue = TestUtils.kolmogorovSmirnovTest(sample1, sample2);
        if (pValue > 1.0) {
            //System.err.println("pvalue > 1.0! " + pValue);
        }
        
        if (pValue <= significance_level) {
            return false;
        } else {
            return true;
        }
        
    }
    
    public static boolean sameStandardDeviation(double[] sample1, double[] sample2, double significance_level) {
        return sameStandardDeviationLeveneTest(significance_level, "MEAN", sample1, sample2);
    }
    
    /**
     * https://en.wikipedia.org/wiki/Levene%27s_test
     *  
     * @param significance_level
     * @param samples
     * @return
     */
    public static boolean sameStandardDeviationLeveneTest(double significance_level, String leveneType, double[]... samples ) {
        //number of groups
        int k = 0;
        int n = 0;
        for (double[] sample: samples) {
            k++;
            n+= sample.length;
        }
        //mean, median or 
        double[] allYs = new double[k];
        for (int i = 0; i < k; i++) {
            double[] samplei = samples[i];
            if (leveneType.equals("MEAN")) {
                allYs[i] = StatUtils.mean(samplei);
            } else if (leveneType.equals("MEDIAN")) {
                allYs[i] = new DescriptiveStatistics(samplei).getPercentile(50);
            }
            
        }
        
        double allZs[][] = new double[k][];
        for (int i = 0; i < k; i++) {
            allZs[i] = new double[samples[i].length];
            for (int j = 0; j < allZs[i].length; j++) {
                allZs[i][j] = Math.abs(samples[i][j] - allYs[i]);
            }
        }
        
        double averageZ = 0;
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < allZs[i].length; j++) {
                averageZ += allZs[i][j];
            }
        }
        averageZ = averageZ / n;
        
        double[] averageZPerGroup = new double[k];
        for (int i = 0; i < k; i++) {
            averageZPerGroup[i] = 0;
            for (int j = 0; j < allZs[i].length; j++) {
                averageZPerGroup[i] += allZs[i][j];
            }
            averageZPerGroup[i] = averageZPerGroup[i] / allZs[i].length;
        }
        
        
        double testStatisticEnumerator = 0;
        for (int i = 0; i < k; i++) {
            testStatisticEnumerator += allZs[i].length * Math.pow(averageZPerGroup[i] - averageZ, 2);
        }
        testStatisticEnumerator = testStatisticEnumerator * (n - k);
        
        double testStatisticDenominator = 0;
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < allZs[i].length; j++) {
                testStatisticDenominator += Math.pow(allZs[i][j] - averageZPerGroup[i], 2);
            }
        }
        testStatisticDenominator = testStatisticDenominator * (k - 1);
        
        double testStatistic = testStatisticEnumerator / testStatisticDenominator;
        
        FDistribution fDistribution = new FDistribution(k -1, n -k);
        
        double pValue = 1.0 - fDistribution.cumulativeProbability(testStatistic);
        
        if (pValue <= significance_level) {
            return false;
        } else {
            return true;
        }
        
    }
   

}
