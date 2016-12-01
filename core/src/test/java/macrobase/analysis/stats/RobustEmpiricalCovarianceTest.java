package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RobustEmpiricalCovarianceTest {
    private static final Logger log = LoggerFactory.getLogger(RobustEmpiricalCovarianceTest.class);

    @Test
    public void testFindOutliers() {
        int k = 5;
        int n = 50000;
        int noiseFreq = 50;
        double outlierRate = 0.01;

        Random r = new Random(0);
        List<Datum> testData = new ArrayList<>();
        for (int i = 0; i < n; ++i) {
            double[] sample = new double[k];
            for (int j = 0; j < k; j++) {
                if (i % noiseFreq == 0) {
                    sample[j] = 100*r.nextDouble();
                } else {
                    sample[j] = (j+1)*r.nextGaussian();
                }
            }
            testData.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample)));
        }

        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.MCD_STOPPING_DELTA, 0.0001)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.METRICS, Arrays.asList(new String[k]));
        MinCovDet mcd = new MinCovDet(conf);

        long startTime = System.currentTimeMillis();
        mcd.train(testData);
        long endTime = System.currentTimeMillis();
        log.debug("MCD Trained on {} in {}", n, endTime-startTime);

        RobustEmpiricalCovariance rcov = new RobustEmpiricalCovariance(conf);
        startTime = System.currentTimeMillis();
        rcov.train(testData);
        endTime = System.currentTimeMillis();
        log.debug("RCOV Trained on {} in {}", n, endTime-startTime);

        double[] mcdScores = new double[n];
        startTime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            mcdScores[i] = mcd.score(testData.get(i));
        }
        endTime = System.currentTimeMillis();
        log.debug("MCD Scored on {} in {}", n, endTime-startTime);

        double[] rcovScores = new double[n];
        startTime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            rcovScores[i] = rcov.score(testData.get(i));
        }
        endTime = System.currentTimeMillis();
        log.debug("RCOV Scored on {} in {}", n, endTime-startTime);

        Percentile pCalc = new Percentile();
        double mcdThreshold = pCalc.evaluate(mcdScores, (1-outlierRate) * 100);
        double rcovThreshold = pCalc.evaluate(rcovScores, (1-outlierRate) * 100);
        int numAgree = 0;
        for (int i = 0; i < n; i++) {
            if ((mcdScores[i] > mcdThreshold) == (rcovScores[i] > rcovThreshold)) {
                numAgree++;
            }
        }
        assertTrue(n - numAgree < 50);
    }
}
