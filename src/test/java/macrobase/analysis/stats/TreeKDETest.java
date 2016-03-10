package macrobase.analysis.stats;

import com.google.common.collect.Lists;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TreeKDETest {

    @Test
    public void bimodalPipeline1DTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "TREE_KDE")
                .set(MacroBaseConf.KDE_KERNEL_TYPE, "EPANECHNIKOV_MULTIPLICATIVE")
                .set(MacroBaseConf.KDE_BANDWIDTH_ALGORITHM, "OVERSMOOTHED")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/verySimple.csv")
                .set(MacroBaseConf.HIGH_METRICS, "x")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "")
                .set(MacroBaseConf.KDTREE_LEAF_CAPACITY, 2);

        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        TreeKDE kde = new TreeKDE(conf);

        assertEquals(15, data.size());
        kde.train(data);

        for (Datum datum : data) {
            assertTrue(-1 * kde.score(datum) > 0);
        }
    }

    @Test
    public void compareWithKDETest() throws ConfigurationException, IOException, SQLException {
        double accuracy = 0.001;
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "TREE_KDE")
                .set(MacroBaseConf.KDE_KERNEL_TYPE, "EPANECHNIKOV_MULTIPLICATIVE")
                .set(MacroBaseConf.KDE_BANDWIDTH_ALGORITHM, "NORMAL_SCALE")
                .set(MacroBaseConf.TREE_KDE_ACCURACY, String.format("%f", accuracy))
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2d_standard_normal_100k.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");

        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        assertEquals(100000, data.size());

        TreeKDE treekde = new TreeKDE(conf);
        treekde.setApproximateLeaves(false);
        treekde.train(data);

        KDE kde = new KDE(conf);
        kde.setProportionOfDataToUse(1.0);
        kde.train(data);

        for (int i=0; i<100; i++) {
            int index = ThreadLocalRandom.current().nextInt(0, data.size() + 1);
            assertEquals(kde.score(data.get(index)), treekde.score(data.get(index)), accuracy);
        }
    }

    @Test
    public void standardNormal2DTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "TREE_KDE")
                .set(MacroBaseConf.KDE_KERNEL_TYPE, "EPANECHNIKOV_MULTIPLICATIVE")
                .set(MacroBaseConf.KDE_BANDWIDTH_ALGORITHM, "NORMAL_SCALE")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2d_standard_normal_100k.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");

        List<Datum> data = Lists.newArrayList(conf.constructIngester());

        TreeKDE kde = new TreeKDE(conf);
        kde.setApproximateLeaves(false);

        assertEquals(100000, data.size());
        kde.train(data);

        double[][] candidates = {
                {0, 0},
                {1, 1},
                {-1, 1},
                {0.2, 0.7},
                {0.8, 0.7},
                {0.4, 0.1},
                {0, 2},
                {0, 0.7},
                {-1, -1},
                {1, -1},
                {0.5, 0.5},
                {20, 20},
        };

        double gaussianNorm = Math.pow(2 * Math.PI, -0.5 * candidates[0].length);
        List<Integer> dummyAttr = new ArrayList<>();

        for (double[] array : candidates) {
            RealVector vector = new ArrayRealVector(array);
            double expectedScore = gaussianNorm * Math.pow(Math.E, -0.5 * vector.getNorm());
            // Accept 10x error or 1e-4 since we don't care about absolute performance only order of magnitude.
            double error = Math.max(1e-4, expectedScore * 0.9);
            double score = -kde.score(new Datum(dummyAttr, vector));

            assertTrue(String.format("expected abs(%f - %f) < %f", expectedScore, score, error),
                    Math.abs(expectedScore - score) < error);
        }
    }

    @Test
    public void simpleCompareWithKDETest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "TREE_KDE")
                .set(MacroBaseConf.KDE_KERNEL_TYPE, "EPANECHNIKOV_MULTIPLICATIVE")
                .set(MacroBaseConf.KDE_BANDWIDTH_ALGORITHM, "NORMAL_SCALE")
                .set(MacroBaseConf.TREE_KDE_ACCURACY, "1e-6")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/20points.csv")
                .set(MacroBaseConf.HIGH_METRICS, "XX")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");

        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        assertEquals(20, data.size());

        TreeKDE treekde = new TreeKDE(conf);
        treekde.setApproximateLeaves(false);
        treekde.train(data);

        KDE kde = new KDE(conf);
        kde.setProportionOfDataToUse(1.0);
        kde.train(data);

       for (Datum datum : data) {
           assertEquals(kde.score(datum), treekde.score(datum), 1e-8);
       }
    }
}
