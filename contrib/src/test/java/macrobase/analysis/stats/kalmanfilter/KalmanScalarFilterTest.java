package macrobase.analysis.stats.kalmanfilter;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import macrobase.util.Drainer;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;

public class KalmanScalarFilterTest {
    private static final Logger log = LoggerFactory.getLogger(KalmanScalarFilterTest.class);

    @Test
    public void reduceToVectorKalmanFilterTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 4)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2gaussians-500points.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(500, data.size());

        double qSacle = 1e-6;

        KalmanVectorFilter vf = new KalmanVectorFilter(new ArrayRealVector(1), qSacle);
        KalmanScalarFilter sf = new KalmanScalarFilter(0, qSacle);

        List<Datum> oneCluster = data.subList(201, 500);
        List<RealVector> vectorFiltered = oneCluster.stream().map(d -> vf.step(d.metrics().getSubVector(0, 1), 1)).collect(Collectors.toList());
        List<Double> scalarFiltered = oneCluster.stream()
                .map(d -> sf.step(d.metrics().getEntry(0), 1))
                .collect(Collectors.toList());

        for (int i=0; i<10; i++) {
            int ri = conf.getRandom().nextInt(300);
            assertEquals(vectorFiltered.get(ri).getEntry(0), scalarFiltered.get(ri));
        }
    }
}
