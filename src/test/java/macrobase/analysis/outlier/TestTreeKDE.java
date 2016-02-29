package macrobase.analysis.outlier;

import macrobase.analysis.BaseAnalyzer;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataLoader;
import macrobase.ingest.DatumEncoder;
import org.junit.Test;
import static org.junit.Assert.*;

import javax.crypto.Mac;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class TestTreeKDE {

    @Test
    public void bimodalPipeline1DTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.DETECTOR_TYPE, "TREE_KDE")
                .set(MacroBaseConf.KDE_KERNEL_TYPE, "EPANECHNIKOV_MULTIPLICATIVE")
                .set(MacroBaseConf.KDE_BANDWIDTH_ALGORITHM, "OVERSMOOTHED")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/verySimple.csv")
                .set(MacroBaseConf.HIGH_METRICS, "x")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "")
                .set(MacroBaseConf.DATA_TRANSFORM_TYPE, "IDENTITY")
                .set(MacroBaseConf.KDTREE_LEAF_CAPACITY, 2);


        BaseAnalyzer analyzer = new BaseAnalyzer(conf);

        DataLoader loader = analyzer.constructLoader();
        List<Datum> data = loader.getData(new DatumEncoder());

        TreeKDE kde = new TreeKDE(conf);

        assertEquals(15, data.size());
        kde.train(data);

        for (Datum datum : data) {
            assertTrue(-1 * kde.score(datum) > 0);
        }

    }
}
