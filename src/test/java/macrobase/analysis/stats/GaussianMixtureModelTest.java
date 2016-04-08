package macrobase.analysis.stats;

import com.google.common.collect.Lists;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class GaussianMixtureModelTest {
    private static final Logger log = LoggerFactory.getLogger(GaussianMixtureModelTest.class);

    @Test
    /**
     * Tests Gaussian Mixture Model on a three well separated clusters.
     */
    public void bivariateWellSeparatedNormalTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "GAUSSIAN_MIXTURE_EM")
                .set(MacroBaseConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-700points.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        assertEquals(700, data.size());

        GaussianMixtureModel gmm = new GaussianMixtureModel(conf);
        gmm.train(data);

        double[][] clusterMeans = {
                {2, 11},
                {1, 1},
                {10, 3},
        };
        List<RealVector> vectorClusterMeans = new ArrayList<>(3);
        for (int k =0; k< 3; k++) {
            vectorClusterMeans.add(new ArrayRealVector(clusterMeans[k]));
        }
        double [][][] clusterCovariances = {
                {{0.5,0.4},{0.4,0.5}},
                {{0.3,0},{0,0.6}},
                {{0.9,0.2},{0.2,0.3}},
        };

        List<RealVector> calculatedMeans = gmm.getMeans();
        List<RealMatrix> calculatedCovariances = gmm.getCovariance();

        for (int i=0; i<3 ; i ++) {
            boolean identified = false;
            for (int j=0; j<3; j++) {
                if (calculatedMeans.get(i).getDistance(vectorClusterMeans.get(j)) < 0.1 ) {
                    for (int p=0; p<2; p++) {
                        for (int q=0; q<2; q++) {
                            // Make sure covariance is in the ballpark. Since we only had 700 points.
                            assertEquals(clusterCovariances[j][p][q], calculatedCovariances.get(i).getEntry(p,q), 0.7);
                        }
                    }
                    identified = true;
                    break;
                }
            }
            assertEquals(true, identified);
        }
    }

    @Test
    /**
     * Tests Gaussian Mixture Model on a three not so well separated clusters.
     */
    public void bivariateOkSeparatedNormalTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "GAUSSIAN_MIXTURE_EM")
                .set(MacroBaseConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-7000points.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        assertEquals(7000, data.size());

        GaussianMixtureModel gmm = new GaussianMixtureModel(conf);
        gmm.train(data);

        double[][] clusterMeans = {
                {1.5, 2},
                {2, 0},
                {4.5, 1},
        };
        List<RealVector> vectorClusterMeans = new ArrayList<>(3);
        for (int k =0; k< 3; k++) {
            vectorClusterMeans.add(new ArrayRealVector(clusterMeans[k]));
        }
        double [][][] clusterCovariances = {
                {{0.5,0.4},{0.4,0.5}},
                {{0.3,0},{0,0.6}},
                {{0.9,0.2},{0.2,0.3}},
        };

        List<RealVector> calculatedMeans = gmm.getMeans();
        List<RealMatrix> calculatedCovariances = gmm.getCovariance();

        for (int i=0; i<3 ; i ++) {
            boolean identified = false;
            for (int j=0; j<3; j++) {
                if (calculatedMeans.get(i).getDistance(vectorClusterMeans.get(j)) < 0.1 ) {
                    for (int p=0; p<2; p++) {
                        for (int q=0; q<2; q++) {
                            assertEquals(clusterCovariances[j][p][q], calculatedCovariances.get(i).getEntry(p,q), 0.1);
                        }
                    }
                    identified = true;
                    break;
                }
            }
            assertEquals("a cluster was not identified", true, identified);
        }
    }
}
