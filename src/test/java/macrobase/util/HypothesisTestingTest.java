package macrobase.util;
import org.junit.Test;

import macrobase.datamodel.Datum;
import macrobase.util.BitSetUtil;
import macrobase.util.HypothesisTesting;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.commons.math3.distribution.*;
import org.apache.commons.math3.linear.ArrayRealVector;

public class HypothesisTestingTest {

    @Test 
    public void sameProportionTestTest() {
        assertEquals( HypothesisTesting.sameProportionTest(100, 0.4, 0.41, 0.05) , true);
        assertEquals( HypothesisTesting.sameProportionTest(100, 0.4, 0.35, 0.05) , true);

        assertEquals( HypothesisTesting.sameProportionTest(100, 0.4, 0.5, 0.05) , false);

    }
    
    
    @Test 
    public void samePosNegTest() {
        assertEquals(HypothesisTesting.samePosNeg(100, 99, 0.05), true);

        assertEquals(HypothesisTesting.samePosNeg(100, 14, 0.05), false);
        
        assertEquals(HypothesisTesting.samePosNeg(1000, 980, 0.05), true);

    }
    
    
    @Test
    public void independenceTest() {
       //T T: 25
       //T F: 25
       //F T: 25
       //F F: 25
       Integer[] values1 = new Integer[100];
       for (int i = 0; i < 50; i++) {
           values1[i] = 1;
       }
       for (int i = 50; i < 100; i++) {
           values1[i] = 0;
       }
       
       Integer[] values2 = new Integer[100];
       for (int i = 0; i < 25; i++) {
           values2[i] = 1;
       }
       for (int i = 25; i < 50; i++) {
           values2[i] = 0;
       }
       for (int i = 50; i < 75; i++) {
           values2[i] = 1;
       }
       for (int i = 75; i < 100; i++) {
           values2[i] = 0;
       }
       
       assertEquals(HypothesisTesting.independenceTest(values1, values2, 0.05),true);


    }
    
    @Test
    public void independenceTest2() {
       //first variable is: A = a1
        //second variable is metric variable
        //P, M
       //T T: 25
       //T F: 25
       //F T: 2500
       //F F: 2500
       // Pr(M = T) = Pr (M = T | P = T)
       // => Pr(M = T) * Pr (P = T) = Pr (M = T and P = T) 
        // => Pr (P = T) = Pr (P = T | M = T)
        
        //seems independent
        //but statistical testing tells me not independent!!
       Integer[] values1 = new Integer[5050];
       for (int i = 0; i < 50; i++) {
           values1[i] = 1;
       }
       for (int i = 50; i < 5050; i++) {
           values1[i] = 0;
       }
       
       Integer[] values2 = new Integer[5050];
       for (int i = 0; i < 25; i++) {
           values2[i] = 1;
       }
       for (int i = 25; i < 50; i++) {
           values2[i] = 0;
       }
       for (int i = 50; i < 2550; i++) {
           values2[i] = 1;
       }
       for (int i = 2550; i < 5050; i++) {
           values2[i] = 0;
       }
       
       assertEquals(HypothesisTesting.independenceTest(values1, values2, 0.05),true);

       long[][] counts = new long[2][2];
       counts[0][0] = 25;
       counts[0][1] = 25;
       counts[1][0] = 2500;
       counts[1][1] = 2500;
       boolean independent = !TestUtils.chiSquareTest(counts, 0.05); 
       assertEquals(independent, true );
    }
    
    
    @Test
    public void testSameDistribution2() throws IOException {
        /*
        String content1 = new String(Files.readAllBytes(Paths.get("/Users/xuchu/parent.txt")));
        String[] splits1 = content1.split("\n");
        double[] values1 = new double[splits1.length];
        for (int i = 0; i < splits1.length; i++) {
            values1[i] = Double.valueOf(splits1[i]);
        }
        
        double sampleMean1 = StatUtils.mean(values1);
        double sampleVariance1 = StatUtils.variance(values1);
        double sd1 = Math.sqrt(sampleVariance1);
        for (double v: values1) {
            double away = Math.abs(v - sampleMean1) / sd1;
            if (away > 5) {
                System.out.println("there are outliers in parent");
            }
        }
        
        String content2 = new String(Files.readAllBytes(Paths.get("/Users/xuchu/child.txt")));
        String[] splits2 = content2.split("\n");
        double[] values2 = new double[splits2.length];
        for (int i = 0; i < splits2.length; i++) {
            values2[i] = Double.valueOf(splits2[i]);
        }
        
        double sampleMean2 = StatUtils.mean(values2);
        double sampleVariance2 = StatUtils.variance(values2);
        double sd2 = Math.sqrt(sampleVariance2);
        for (double v: values2) {
            double away = Math.abs(v - sampleMean2) / sd2;
            if (away > 5) {
                System.out.println("there are outliers in child");
            }
        }
        
        
        boolean sameMean = HypothesisTesting.sameMean(values1, values2, 0.05);
        boolean sameVariance = HypothesisTesting.sameStandardDeviation(values1, values2, 0.05);
        boolean sameKD = HypothesisTesting.sameDistributionKS(values1, values2, 0.05);
        System.out.println("test");
        */
    }
    
    private double[] doubleTheArray(double[] values1) {
        double[] values3 = new double[values1.length * 2];
        for (int i = 0; i < values3.length; i++) {
            if (i < values1.length) {
                values3[i] = values1[i];
            } else {
                values3[i] = values1[i - values1.length];
            }
        }
        return values3;
    }
    
    @Test
    public void testSameMean() throws IOException {
       
        double[] sample1 = new double[100];
        for (int i = 0; i < sample1.length; i++) {
            sample1[i] = i * 10;
        }
        double[] sample2 = new double[100];
        for (int i = 0; i < sample2.length; i++) {
            sample2[i] = (i + 1)*10;
        }
        
        boolean sameMean = HypothesisTesting.sameMean(sample1, sample2, 0.05);
        boolean sameMean2 = HypothesisTesting.sameMean(sample2, sample1, 0.05);
        assertEquals(sameMean, true);
        
        double[] sample3 = new double[]{1,2,10,4,5,6,1,4,55,5,123};

        double[] sample4 = new double[]{10,43754,328,499,1};
        HypothesisTesting.sameMean(sample3, sample4, 0.05);
       
    }
    
    
    
    private double[] randomSampling(double[] population, int numSample) {
        List<Integer> sampleDataIndexes = new ArrayList<Integer>();
        Random rnd = new Random();
        for (int i = 0; i < population.length; i++) {
            if (sampleDataIndexes.size() < numSample) {
                sampleDataIndexes.add(i);
            } else {
                int j = rnd.nextInt(i); //j in [0,i)
                if (j < sampleDataIndexes.size()) {
                    sampleDataIndexes.set(j, i);
                }
            }
        }
        
        double[] sample = new double[numSample];
        for (int i = 0; i < numSample; i++) {
            sample[i] = population[sampleDataIndexes.get(i)];
        }
        return sample;
    }
    
    @Test
    public void testSameVariance() throws IOException {
        double[] population = new double[10000];
        for(int i = 0; i < population.length; i++) {
            population[i] = i;
        }
        double[] sample1 = new double[100];
        for (int i = 0; i < sample1.length; i++) {
            sample1[i] = i;
        }
        double[] sample2 = new double[100];
        for (int i = 0; i < sample2.length; i++) {
            sample2[i] = i;
        }
        boolean sameVariance = HypothesisTesting.sameStandardDeviation(sample1, sample2, 0.05);
        assertEquals(sameVariance,true);
      
    }
    
    @Test
    public void testTDistribution() throws IOException {
        // http://stattrek.com/hypothesis-test/difference-in-means.aspx?Tutorial=AP
        double degreeOfFreedom = 40.47;
        TDistribution tDistribution = new TDistribution(degreeOfFreedom);
        double cd = tDistribution.cumulativeProbability(-1.99);
        
        assertEquals(cd, 0.027, 1e-3);
      
    }
    
    @Test
    public void testHypothesisTestingSameMean() throws IOException {
        // http://www.itl.nist.gov/div898/handbook/eda/section3/eda353.htm
        double[] sample1 = new double[]{18,15,18,16,17,15,14,14,14,15,15,14,15,14,22,18,21,21,10,10,11,9,28,25,19,16,17,19,18,14,14,14,14,12,13,13,18,22,19,18,23,26,25,20,21,13,14,15,14,17,11,13,12,13,15,13,13,14,22,28,13,14,13,14,15,12,13,13,14,13,12,13,18,16,18,18,23,11,12,13,12,18,21,19,21,15,16,15,11,20,21,19,15,26,25,16,16,18,16,13,14,14,14,28,19,18,15,15,16,15,16,14,17,16,15,18,21,20,13,23,20,23,18,19,25,26,18,16,16,15,22,22,24,23,29,25,20,18,19,18,27,13,17,13,13,13,30,26,18,17,16,15,18,21,19,19,16,16,16,16,25,26,31,34,36,20,19,20,19,21,20,25,21,19,21,21,19,18,19,18,18,18,30,31,23,24,22,20,22,20,21,17,18,17,18,17,16,19,19,36,27,23,24,34,35,28,29,27,34,32,28,26,24,19,28,24,27,27,26,24,30,39,35,34,30,22,27,20,18,28,27,34,31,29,27,24,23,38,36,25,38,26,22,36,27,27,32,28,31};
        double[] sample2 = new double[]{24,27,27,25,31,35,24,19,28,23,27,20,22,18,20,31,32,31,32,24,26,29,24,24,33,33,32,28,19,32,34,26,30,22,22,33,39,36,28,27,21,24,30,34,32,38,37,30,31,37,32,47,41,45,34,33,24,32,39,35,32,37,38,34,34,32,33,32,25,24,37,31,36,36,34,38,32,38,32};
        boolean sameMeans = HypothesisTesting.sameMean(sample1, sample2, 0.1);
        assertEquals(sameMeans, false);
    }
    
    @Test
    public void testHypothesisTestingLeveneTest() {
        //http://www.itl.nist.gov/div898/handbook/eda/section3/eda35a.htm
        double[] allPoints = new double[]{1.006,0.996,0.998,1,0.992,0.993,1.002,0.999,0.994,1,0.998,1.006,1,1.002,0.997,0.998,0.996,1,1.006,0.988,0.991,0.987,0.997,0.999,0.995,0.994,1,0.999,0.996,0.996,1.005,1.002,0.994,1,0.995,0.994,0.998,0.996,1.002,0.996,0.998,0.998,0.982,0.99,1.002,0.984,0.996,0.993,0.98,0.996,1.009,1.013,1.009,0.997,0.988,1.002,0.995,0.998,0.981,0.996,0.99,1.004,0.996,1.001,0.998,1,1.018,1.01,0.996,1.002,0.998,1,1.006,1,1.002,0.996,0.998,0.996,1.002,1.006,1.002,0.998,0.996,0.995,0.996,1.004,1.004,0.998,0.999,0.991,0.991,0.995,0.984,0.994,0.997,0.997,0.991,0.998,1.004,0.997};
        int k = 10;
        double[] sample1 = new double[10];
        for (int i = 0; i < 10; i++) {
            sample1[i] = allPoints[i];
        }
        double[] sample2 = new double[10];
        for (int i = 0; i < 10; i++) {
            sample2[i] = allPoints[10 + i];
        }
        double[] sample3 = new double[10];
        for (int i = 0; i < 10; i++) {
            sample3[i] = allPoints[20 + i];
        }
        double[] sample4 = new double[10];
        for (int i = 0; i < 10; i++) {
            sample4[i] = allPoints[30 + i];
        }
        double[] sample5 = new double[10];
        for (int i = 0; i < 10; i++) {
            sample5[i] = allPoints[40 + i];
        }
        double[] sample6 = new double[10];
        for (int i = 0; i < 10; i++) {
            sample6[i] = allPoints[50 + i];
        }
        double[] sample7 = new double[10];
        for (int i = 0; i < 10; i++) {
            sample7[i] = allPoints[60 + i];
        }
        double[] sample8 = new double[10];
        for (int i = 0; i < 10; i++) {
            sample8[i] = allPoints[70 + i];
        }
        double[] sample9 = new double[10];
        for (int i = 0; i < 10; i++) {
            sample9[i] = allPoints[80 + i];
        }
        double[] sample10 = new double[10];
        for (int i = 0; i < 10; i++) {
            sample10[i] = allPoints[90 + i];
        }
        boolean sameVariance = HypothesisTesting.sameStandardDeviationLeveneTest(0.05, "MEDIAN", 
                sample1,sample2,sample3,sample4,sample5,sample6,sample7,sample8,sample9,sample10);
        assertEquals(sameVariance,true);
    }
    @Test
    public void testHypothesisTestingLeveneTest2() {
        //http://www.itl.nist.gov/div898/handbook/eda/section3/eda35a.htm
       
        double[] sample1 = new double[100];
        for (int i = 0; i < 100; i++) {
            sample1[i] = i;
        }
        double[] sample2 = new double[100];
        for (int i = 0; i < 100; i++) {
            sample2[i] = i;
        }
        
        boolean sameVariance12_median = HypothesisTesting.sameStandardDeviationLeveneTest(0.05, "MEDIAN", 
                sample1,sample2);
        assertEquals(sameVariance12_median,true);
        boolean sameVariance12_mean = HypothesisTesting.sameStandardDeviationLeveneTest(0.05, "MEAN", 
                sample1,sample2);
        assertEquals(sameVariance12_mean,true);
        
        double[] sample3 = new double[100];
        for (int i = 0; i < 100; i++) {
            sample3[i] = i;
        }
        double[] sample4 = new double[100];
        for (int i = 0; i < 100; i++) {
            sample4[i] = Math.pow(i, 2);
        }
        
        boolean sameVariance34_median = HypothesisTesting.sameStandardDeviationLeveneTest(0.05, "MEDIAN", 
                sample3,sample4);
        assertEquals(sameVariance34_median,false);
        
        boolean sameVariance34_mean = HypothesisTesting.sameStandardDeviationLeveneTest(0.05, "MEAN", 
                sample3,sample4);
        assertEquals(sameVariance34_mean,false);
        
    }
}
