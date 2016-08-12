package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;

public class BayesianNormalDensityTest {
    private static final Logger log = LoggerFactory.getLogger(BayesianNormalDensityTest.class);

    @Test
    public void univariateCompareWithKDETest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "BAYESIAN_NORMAL")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2d_standard_normal_100k.csv.gz")
                .set(MacroBaseConf.METRICS, "XX")
                .set(MacroBaseConf.ATTRIBUTES, "");

        List<Datum> data = conf.constructIngester().getStream().drain();
        assertEquals(100000, data.size());

        BayesianNormalDensity bayesianNormal = new BayesianNormalDensity(conf);
        bayesianNormal.train(data);

        KDE kde = new KDE(conf);
        kde.setProportionOfDataToUse(1.0);
        kde.train(data);

        Datum d;
        int index;
        Random rand = new Random();
        for (int i =0 ; i < 33; i++ ) {
            index = rand.nextInt(data.size());
            d = data.get(index);
            assertEquals(kde.score(d), -bayesianNormal.getDensity(d), 0.03);
        }
    }

    @Test
    public void univariateNormalTest() throws Exception {
        // Make sure we are close to standard normal
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "BAYESIAN_NORMAL")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2d_standard_normal_100k.csv.gz")
                .set(MacroBaseConf.METRICS, "XX")
                .set(MacroBaseConf.ATTRIBUTES, "");

        NormalDistribution standardNormal = new NormalDistribution(0, 1);

        List<Datum> data = conf.constructIngester().getStream().drain();
        assertEquals(100000, data.size());

        BayesianNormalDensity bayesianNormal = new BayesianNormalDensity(conf);
        bayesianNormal.train(data);

        assertEquals(0, bayesianNormal.getMean().getEntry(0), 0.01);

        Datum d;
        int index;
        Random rand = new Random();
        for (int i =0 ; i < 100; i++ ) {
            index = rand.nextInt(data.size());
            d = data.get(index);
            assertEquals(standardNormal.density(d.metrics().getEntry(0)), bayesianNormal.getDensity(d), 0.01);
        }
    }

    @Test
    public void bivariateNormalTest() throws Exception {
        // Make sure we are close to bivariate normal
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "BAYESIAN_NORMAL")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2d_standard_normal_100k.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");

        double[] means = {0, 0};
        double[][] variance = {{1, 0}, {0, 1}};
        MultivariateNormalDistribution bivariateNormal = new MultivariateNormalDistribution(means, variance);

        List<Datum> data = conf.constructIngester().getStream().drain();
        assertEquals(100000, data.size());

        BayesianNormalDensity bayesianNormal = new BayesianNormalDensity(conf);
        bayesianNormal.train(data);

        assertEquals(0, bayesianNormal.getMean().getEntry(0), 0.01);
        assertEquals(0, bayesianNormal.getMean().getEntry(1), 0.01);

        Datum d;
        int index;
        Random rand = new Random();
        for (int i =0 ; i < 100; i++ ) {
            index = rand.nextInt(data.size());
            d = data.get(index);
            assertEquals(bivariateNormal.density(d.metrics().toArray()), bayesianNormal.getDensity(d), 1e-3);
        }
    }
}
