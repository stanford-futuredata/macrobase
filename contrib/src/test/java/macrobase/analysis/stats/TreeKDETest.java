package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import macrobase.util.Drainer;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;

public class TreeKDETest {

    @Test
    public void bimodalPipeline1DTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(TreeKDE.KDE_KERNEL_TYPE, "EPANECHNIKOV_MULTIPLICATIVE")
                .set(TreeKDE.KDE_BANDWIDTH_ALGORITHM, "OVERSMOOTHED")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/verySimple.csv")
                .set(MacroBaseConf.METRICS, "x")
                .set(MacroBaseConf.ATTRIBUTES, "")
                .set(TreeKDE.KDTREE_LEAF_CAPACITY, 2);

        List<Datum> data = Drainer.drainIngest(conf);
        TreeKDE kde = new TreeKDE(conf);

        assertEquals(15, data.size());
        kde.train(data);

        for (Datum datum : data) {
            assertThat(kde.score(datum), Matchers.greaterThan(Double.valueOf(0)));
        }
    }

    @Test
    public void compareWithKDETest() throws Exception {
        double accuracy = 0.001;
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "TREE_KDE")
                .set(TreeKDE.KDE_KERNEL_TYPE, "EPANECHNIKOV_MULTIPLICATIVE")
                .set(TreeKDE.KDE_BANDWIDTH_ALGORITHM, "NORMAL_SCALE")
                .set(TreeKDE.TREE_KDE_ACCURACY, String.format("%f", accuracy))
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2d_standard_normal_100k.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");

        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(100000, data.size());

        TreeKDE treekde = new TreeKDE(conf);
        treekde.setApproximateLeaves(false);
        treekde.train(data);

        KDE kde = new KDE(conf);
        kde.setProportionOfDataToUse(1.0);
        kde.train(data);

        Random r = new Random(0);
        for (int i=0; i<100; i++) {
            int index = r.nextInt(data.size() + 1);
            Datum datum = data.get(index);
            double kde_score = kde.score(datum);
            double treekde_score = treekde.scoreDensity(datum);
            assertEquals(
                    kde_score,
                    treekde_score,
                    accuracy);
        }
    }

    @Test
    public void standardNormal2DTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(TreeKDE.KDE_KERNEL_TYPE, "EPANECHNIKOV_MULTIPLICATIVE")
                .set(TreeKDE.KDE_BANDWIDTH_ALGORITHM, "NORMAL_SCALE")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2d_standard_normal_100k.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");

        List<Datum> data = Drainer.drainIngest(conf);

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
            double expectedScore = gaussianNorm * Math.exp(-0.5 * vector.getNorm());
            // Accept 10x error or 1e-4 since we don't care about absolute performance only order of magnitude.
            double error = Math.max(1e-4, expectedScore * 0.9);
            double score = -kde.scoreDensity(new Datum(dummyAttr, vector));

            assertEquals(
                    expectedScore,
                    score,
                    error
            );
        }
    }

    @Test
    public void simpleCompareWithKDETest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(TreeKDE.KDE_KERNEL_TYPE, "EPANECHNIKOV_MULTIPLICATIVE")
                .set(TreeKDE.KDE_BANDWIDTH_ALGORITHM, "NORMAL_SCALE")
                .set(TreeKDE.TREE_KDE_ACCURACY, "1e-6")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/20points.csv")
                .set(MacroBaseConf.METRICS, "XX")
                .set(MacroBaseConf.ATTRIBUTES, "");

        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(20, data.size());

        TreeKDE treekde = new TreeKDE(conf);
        treekde.setApproximateLeaves(false);
        treekde.train(data);

        KDE kde = new KDE(conf);
        kde.setProportionOfDataToUse(1.0);
        kde.train(data);

       for (Datum datum : data) {
           assertEquals(kde.score(datum), treekde.scoreDensity(datum), 1e-8);
       }
    }
}
