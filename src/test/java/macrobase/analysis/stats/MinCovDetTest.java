package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
    public void testMahalanobis() {
        int dim = 100;
        int nsamples = 1000;
        Random r = new Random(0);

        List<Datum> testData = new ArrayList<>();

        for (int i = 0; i < nsamples; ++i) {
            double[] sample = new double[dim];
            for (int d = 0; d < dim; ++d) {
                sample[d] = d % 2 == 0 ? r.nextDouble() : r.nextGaussian();
            }
            testData.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample)));
        }

        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.LOW_METRICS, new ArrayList<String>())
                .set(MacroBaseConf.MCD_STOPPING_DELTA, 0.0001)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.HIGH_METRICS, Arrays.asList(new String[dim]));
        MinCovDet trainer = new MinCovDet(conf);

        trainer.train(testData);

        RealMatrix inverseCov = trainer.getInverseCovariance();
        RealVector mean = trainer.getMean();

        for (Datum d : testData) {
            assertEquals(trainer.score(d), getMahalanobisApache(mean, inverseCov, d.getMetrics()), 0.01);
        }
    }
}
