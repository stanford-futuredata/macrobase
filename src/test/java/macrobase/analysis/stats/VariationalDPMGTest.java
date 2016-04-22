package macrobase.analysis.stats;

import com.google.common.collect.Lists;
import macrobase.analysis.stats.mixture.VariationalDPMG;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.JsonUtils;
import macrobase.diagnostics.DensityDumper;
import macrobase.ingest.CSVIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class VariationalDPMGTest {
    private static final Logger log = LoggerFactory.getLogger(VariationalDPMGTest.class);

    @Test
    /**
     * Tests Bayesian Dirichlet Process Gaussian Mixture on a three well separated clusters.
     */
    public void univariateToyBimodalTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 4)
                .set(MacroBaseConf.TRANSFORM_TYPE, "VARIATIONAL_DPMM")
                .set(MacroBaseConf.MIXTURE_MAX_ITERATIONS_TO_CONVERGE, 15)
                .set(MacroBaseConf.DPM_TRUNCATING_PARAMETER, 8)
                .set(MacroBaseConf.DPM_CONCENTRATION_PARAMETER, 2)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/toy2gaussians.csv")
                .set(MacroBaseConf.HIGH_METRICS, "XX")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        assertEquals(18, data.size());

        VariationalDPMG variationalDPGM = new VariationalDPMG(conf);
        variationalDPGM.train(data);
        log.debug("{}",  variationalDPGM.getClusterCenters(), variationalDPGM.getClusterWeights());
    }

    @Test
    /**
     * Tests Bayesian Gaussian Mixture Model on a three well separated clusters.
     */
    public void bivariateWellSeparatedNormalTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 4)
                .set(MacroBaseConf.TRANSFORM_TYPE, "VARIATIONAL_DPMM")
                .set(MacroBaseConf.DPM_TRUNCATING_PARAMETER, 10)
                .set(MacroBaseConf.MIXTURE_MAX_ITERATIONS_TO_CONVERGE, 15)
                .set(MacroBaseConf.DPM_CONCENTRATION_PARAMETER, 1)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-700points.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        assertEquals(700, data.size());

        VariationalDPMG variationalDPGM = new VariationalDPMG(conf);
        variationalDPGM.train(data);

        log.debug("clusters : {}", variationalDPGM.getClusterCenters());
        log.debug("weights: {}", variationalDPGM.getClusterWeights());

        double[][] boundaries = {
                {-1, 13.01},
                {-1, 13.01},
        };

        JsonUtils.dumpAsJson(variationalDPGM.getClusterCovariances(), "VariationalDPMGTest-bivariateWellSeparatedNormalTest-covariances.json");
        JsonUtils.dumpAsJson(variationalDPGM.getClusterCenters(), "VariationalDPMGTest-bivariateWellSeparatedNormalTest-means.json");
        JsonUtils.dumpAsJson(variationalDPGM.getClusterWeights(), "VariationalDPMGTest-bivariateWellSeparatedNormalTest-weights.json");

        conf.set(MacroBaseConf.SCORED_DATA_FILE, "3gaussians-700-grid.json");
        DensityDumper dumper = new DensityDumper(conf);
        dumper.dumpScores(variationalDPGM, boundaries, 0.05);

        conf.set(MacroBaseConf.SCORED_DATA_FILE, "3gaussians-700-data.json");
        dumper = new DensityDumper(conf);
        dumper.dumpScores(variationalDPGM, data);


        double[][] clusterMeans = {
                {2, 11},
                {1, 1},
                {10, 3},
        };
        double[] clusterPoints = {
                200,
                200,
                300,
        };
        List<RealVector> vectorClusterMeans = new ArrayList<>(3);
        for (int k = 0; k < 3; k++) {
            vectorClusterMeans.add(new ArrayRealVector(clusterMeans[k]));
        }

        List<RealVector> calculatedCenters = variationalDPGM.getClusterCenters();
        double[] weights = variationalDPGM.getClusterWeights();

        double[] inferedWeights = new double[3];
        for (int i = 0 ; i < calculatedCenters.size(); i++ ) {
            if (weights[i] < 0.001) {
                continue;
            }
            for (int j=0 ; j < 3 ; j++) {
                if (calculatedCenters.get(i).getDistance(vectorClusterMeans.get(j)) < 2) {
                    log.debug("identified cluster {} to be close to {} (weight = {})", calculatedCenters.get(i), vectorClusterMeans.get(j), weights[i]);
                    inferedWeights[j] += weights[i];
                    break;
                }
            }
        }

        for (int j=0 ; j < 3 ; j++) {
            assertEquals(clusterPoints[j], inferedWeights[j] * 700, 3);
        }
    }

    @Test
    /**
     * Tests Gaussian Mixture Model on a three not so well separated clusters.
     */
    public void bivariateOkSeparatedNormalTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 4)
                .set(MacroBaseConf.TRANSFORM_TYPE, "VARIATIONAL_DPMM")
                .set(MacroBaseConf.MIXTURE_MAX_ITERATIONS_TO_CONVERGE, 15)
                .set(MacroBaseConf.DPM_TRUNCATING_PARAMETER, 20)
                .set(MacroBaseConf.DPM_CONCENTRATION_PARAMETER, 0.1)
                .set(MacroBaseConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-7000points.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        assertEquals(7000, data.size());

        double[][] clusterMeans = {
                {1.5, 2},
                {2, 0},
                {4.5, 1},
        };
        List<RealVector> vectorClusterMeans = new ArrayList<>(3);
        for (int k = 0; k < 3; k++) {
            vectorClusterMeans.add(new ArrayRealVector(clusterMeans[k]));
        }
        double[][][] clusterCovariances = {
                {{0.5, 0.4}, {0.4, 0.5}},
                {{0.3, 0}, {0, 0.6}},
                {{0.9, 0.2}, {0.2, 0.3}},
        };

        double[][] boundaries = {
                {0, 6.01},
                {-2, 4.01},
        };

        VariationalDPMG variationalDPGM = new VariationalDPMG(conf);
        variationalDPGM.train(data);

        JsonUtils.dumpAsJson(variationalDPGM.getClusterCovariances(), "VariationalDPMGTest-bivariateOkSeparatedNormalTest-covariances.json");
        JsonUtils.dumpAsJson(variationalDPGM.getClusterCenters(), "VariationalDPMGTest-bivariateOkSeparatedNormalTest-means.json");
        JsonUtils.dumpAsJson(variationalDPGM.getClusterWeights(), "VariationalDPMGTest-bivariateOkSeparatedNormalTest-weights.json");

        conf.set(MacroBaseConf.SCORED_DATA_FILE, "3gaussians-7k-grid.json");
        DensityDumper dumper = new DensityDumper(conf);
        dumper.dumpScores(variationalDPGM, boundaries, 0.05);

        conf.set(MacroBaseConf.SCORED_DATA_FILE, "3gaussians-7k-data.json");
        dumper = new DensityDumper(conf);
        dumper.dumpScores(variationalDPGM, data);
    }
}
