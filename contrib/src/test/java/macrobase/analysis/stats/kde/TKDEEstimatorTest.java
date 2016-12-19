package macrobase.analysis.stats.kde;

import macrobase.util.data.CSVDataSource;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;

public class TKDEEstimatorTest {

    @Test
    public void check1Percent() throws Exception {
        List<double[]> data = new CSVDataSource(
                "src/test/resources/data/us_energy_sample.csv",
                2
        ).get();
        List<double[]> trueDensities = new CSVDataSource(
                "src/test/resources/data/us_energy_sample_densities.csv",
                1
        ).get();
        double[] tDensities = new double[data.size()];
        TKDEConf tConf = TKDEConf.load(
                "src/test/resources/conf/test_treekde.yaml"
        );

        TKDEEstimator kde = new TKDEEstimator(tConf);
        kde.train(data);

        double[] densities = new double[data.size()];
        for (int i = 0; i < densities.length; i++) {
            densities[i] = kde.density(data.get(i));
            tDensities[i] = trueDensities.get(i)[0];
        }

        Percentile p = new Percentile(1.0);
        double kdeCutoff = p.evaluate(densities);
        double trueCutoff = p.evaluate(tDensities);
        assertThat(kdeCutoff, closeTo(trueCutoff, 0.01 * trueCutoff));

        int disagree = 0;
        for (int i = 0; i < densities.length; i++) {
            boolean curOutlier = densities[i] < kdeCutoff;
            boolean trueOutlier = tDensities[i] < trueCutoff;
            if (curOutlier != trueOutlier) {
                disagree++;
            }
        }
        assertThat(disagree, is(0));
    }
}
