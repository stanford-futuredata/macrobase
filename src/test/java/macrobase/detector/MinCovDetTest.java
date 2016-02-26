package macrobase.detector;

import macrobase.analysis.outlier.CovarianceMatrixAndMean;
import macrobase.analysis.outlier.MinCovDet;
import macrobase.datamodel.Datum;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;


/**
 * Created by pbailis on 1/21/16.
 */
public class MinCovDetTest {
    private static double getMahalanobisApache(RealVector mean,
                                        RealMatrix inverseCov,
                                        RealVector vec) {
        // sqrt((vec-mean)^T S^-1 (vec-mean))
        RealMatrix vecT = new Array2DRowRealMatrix(vec.toArray());
        RealMatrix meanT = new Array2DRowRealMatrix(mean.toArray());
        RealMatrix vecSubtractMean = vecT.subtract(meanT);

        return Math.sqrt(vecSubtractMean.transpose()
                                 .multiply(inverseCov)
                                 .multiply(vecSubtractMean).getEntry(0, 0));
    }
    
    @Test
    public void testParallelCovarianceAndMeanComputation() {
    	int dim = 100;
    	int nsamples = 1000;
    	int numPartitions = 10;
    	Random r = new Random(0);
    	
    	MinCovDet trainer = new MinCovDet(dim);
    	    	
    	List<List<Datum>> testData = new ArrayList<List<Datum>>();
    	List<RealMatrix> covarianceMatrices = new ArrayList<RealMatrix>();
    	List<RealVector> means = new ArrayList<RealVector>();
    	List<Double> allNumSamples = new ArrayList<Double>();
    	for (int j = 0; j < numPartitions; j++) {
    		testData.add(new ArrayList<Datum>());
	    	for(int i = 0; i < nsamples; ++i) {
	            double[] sample = new double[dim];
	            for(int d = 0; d < dim; ++d) {
	                sample[d] = d % 2 == 0 ? r.nextDouble() : r.nextGaussian();
	            }
	            testData.get(j).add(new Datum(new ArrayList<>(), new ArrayRealVector(sample)));
	        }
	    	covarianceMatrices.add(trainer.getCovariance(testData.get(j)));
	    	means.add(trainer.getMean(testData.get(j)));
	    	allNumSamples.add((double) nsamples);
    	}
    	
    	List<Datum> allTestData = new ArrayList<Datum>();
    	for (int j = 0; j < numPartitions; j++) {
    		for (Datum datum : testData.get(j)) {
    			allTestData.add(datum);
    		}
    	}
    	
    	CovarianceMatrixAndMean res = MinCovDet.combineCovarianceMatrices(covarianceMatrices, means, allNumSamples);
    	RealMatrix actualCovariance = trainer.getCovariance(allTestData);
    	RealVector actualMean = trainer.getMean(allTestData);
    	
    	for (int d = 0; d < dim; d++) {
    		assertTrue(Math.abs(res.getMean().getEntry(d) - actualMean.getEntry(d)) < 1e-4);
    	}
    	
    	for (int d1 = 0; d1 < dim; d1++) {
    		for (int d2 = 0; d2 < dim; d2++) {
    			assertTrue(Math.abs(res.getCovarianceMatrix().getEntry(d1, d2) - actualCovariance.getEntry(d1, d2)) < 1e-4);
    		}
    	}
    }

    /* @Test
    public void testMahalanobis() {
        int dim = 100;
        int nsamples = 1000;
        Random r = new Random(0);

        List<Datum> testData = new ArrayList<>();

        for(int i = 0; i < nsamples; ++i) {
            double[] sample = new double[dim];
            for(int d = 0; d < dim; ++d) {
                sample[d] = d % 2 == 0 ? r.nextDouble() : r.nextGaussian();
            }
            testData.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample)));
        }

        MinCovDet trainer = new MinCovDet(dim);

        trainer.train(testData);

        RealMatrix inverseCov = trainer.getInverseCovariance();
        RealVector mean = trainer.getLocalMean();

        for(Datum d : testData) {
            assertEquals(trainer.score(d), getMahalanobisApache(mean, inverseCov, d.getMetrics()), 0.01);
        }
    } */
}
