package macrobase.analysis.stats.kalmanfilter;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import macrobase.util.AlgebraUtils;
import macrobase.util.Drainer;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;

public class KalmanFlattenedMatrixFilterTest {
    private static final Logger log = LoggerFactory.getLogger(KalmanFlattenedMatrixFilterTest.class);

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

        KalmanVectorFilter f = new KalmanVectorFilter(new ArrayRealVector(2), 1e-6, 1);

        RealMatrix shapeMatrix = new BlockRealMatrix(2, 1);
        KalmanFlattenedMatrixFilter mf = new KalmanFlattenedMatrixFilter(shapeMatrix, 1e-6, 1);

        List<Datum> oneCluster = data.subList(201, 500);
        List<RealVector> vectorFiltered = oneCluster.stream().map(d -> f.step(d.metrics(), 1)).collect(Collectors.toList());
        List<RealMatrix> matrixFiltered = oneCluster.stream()
                .map(d -> mf.step(AlgebraUtils.reshapeMatrixByColumns(d.metrics(), shapeMatrix), 1))
                .collect(Collectors.toList());

        for (int i = 0; i < 10; i++) {
            int ri = conf.getRandom().nextInt(300);
            assertEquals(vectorFiltered.get(ri), AlgebraUtils.flattenMatrixByColumns(matrixFiltered.get(ri)));
        }
    }
}
