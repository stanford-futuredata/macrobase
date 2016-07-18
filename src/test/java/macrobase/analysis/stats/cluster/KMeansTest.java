package macrobase.analysis.stats.cluster;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import macrobase.util.Drainer;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;

public class KMeansTest {
    @Test
    public void simpleTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 10)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/toy2gaussians.csv")
                .set(MacroBaseConf.HIGH_METRICS, "XX")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(18, data.size());

        Random rand = conf.getRandom();

        KMeans kMeans = new KMeans(2);
        List<RealVector> centers = kMeans.emIterate(data, rand);
        assertEquals(centers.get(0).getEntry(0), 106, 1e-7);
        assertEquals(centers.get(1).getEntry(0), 1, 1e-7);
    }

    @Test
    public void twoGTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 10)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2gaussians-500points.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(500, data.size());

        Random rand = conf.getRandom();

        KMeans kMeans = new KMeans(2);
        List<RealVector> centers = kMeans.emIterate(data, rand);
        assertEquals(centers.get(0).getEntry(0), 1, 0.1);
        assertEquals(centers.get(0).getEntry(1), 1, 0.1);
        assertEquals(centers.get(1).getEntry(0), 10, 0.1);
        assertEquals(centers.get(1).getEntry(1), 3, 0.1);
    }
}
