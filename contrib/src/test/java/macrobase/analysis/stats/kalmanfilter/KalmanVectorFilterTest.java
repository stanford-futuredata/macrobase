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

public class KalmanVectorFilterTest {
    private static final Logger log = LoggerFactory.getLogger(KalmanVectorFilterTest.class);

    @Test
    public void runTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 4)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2gaussians-500points.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(500, data.size());

        KalmanVectorFilter f = new KalmanVectorFilter(new ArrayRealVector(2), 1e-6, 1.);

        List<Datum> oneCluster = data.subList(201, 500);
        List<RealVector> filtered = oneCluster.stream().map(d -> f.step(d.metrics(), 1)).collect(Collectors.toList());
        List<Datum> unfilteredlast10 = oneCluster.subList(oneCluster.size() - 10, oneCluster.size());
        List<RealVector> filteredlast10 = filtered.subList(filtered.size() - 10, filtered.size());
        double filteredMax = filteredlast10.stream().mapToDouble(d -> d.getEntry(0)).max().getAsDouble();
        double filteredMin = filteredlast10.stream().mapToDouble(d -> d.getEntry(0)).min().getAsDouble();
        double unfilteredMax = unfilteredlast10.stream().mapToDouble(d -> d.metrics().getEntry(0)).max().getAsDouble();
        double unfilteredMin = unfilteredlast10.stream().mapToDouble(d -> d.metrics().getEntry(0)).min().getAsDouble();
        assert filteredMax < unfilteredMax;
        assert filteredMin > unfilteredMin;
    }
}
