package macrobase.analysis.stats.kde;

import macrobase.util.data.CSVDataSource;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;

public class QuantileBoundEstimatorTest {
    @Test
    public void check1Percent() throws Exception {
        List<double[]> energyData = new CSVDataSource(
                "src/test/resources/data/us_energy_sample.csv",
                2
        ).get();

        TKDEConf conf = new TKDEConf();
        conf.qSampleSize = 20000;
        conf.ignoreSelfScoring = true;

        QuantileBoundEstimator qEstimator = new QuantileBoundEstimator(conf);
        qEstimator.estimateQuantiles(energyData);
        // Value calculated from sklearn
        // bandwidth: [ 49.84778156, 10.74687233]
        assertThat(qEstimator.qT, closeTo(2.4597e-7, 1e-8));
    }
}
