package macrobase.analysis;

import macrobase.analysis.outlier.BinnedKDE;
import macrobase.analysis.outlier.KDE;
import macrobase.analysis.outlier.MAD;
import macrobase.analysis.outlier.MinCovDet;
import macrobase.analysis.outlier.MovingAverage;
import macrobase.analysis.outlier.TreeKDE;
import macrobase.analysis.outlier.ZScore;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.CsvLoader;
import macrobase.ingest.DiskCachingPostgresLoader;
import macrobase.ingest.PostgresLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

public class BaseAnalyzerTest {
    private class DummyAnalyzer extends BaseAnalyzer {
        public DummyAnalyzer(MacroBaseConf conf) throws ConfigurationException {
            super(conf);
        }
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testConstructors() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, new ArrayList<>());
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test;");


        conf.set(MacroBaseConf.DATA_LOADER_TYPE,
                 MacroBaseConf.DataLoaderType.POSTGRES_LOADER);
        assertTrue(new DummyAnalyzer(conf).constructLoader() instanceof PostgresLoader);

        conf.set(MacroBaseConf.DATA_LOADER_TYPE,
                 MacroBaseConf.DataLoaderType.CSV_LOADER);
        conf.set(MacroBaseConf.CSV_INPUT_FILE, folder.newFile("dummy").getAbsolutePath());

        assertTrue(new DummyAnalyzer(conf).constructLoader() instanceof CsvLoader);

        conf.set(MacroBaseConf.DATA_LOADER_TYPE,
                 MacroBaseConf.DataLoaderType.CACHING_POSTGRES_LOADER);
        conf.set(MacroBaseConf.DB_CACHE_DIR, folder.newFolder().getAbsolutePath());
        assertTrue(new DummyAnalyzer(conf).constructLoader() instanceof DiskCachingPostgresLoader);

        conf.set(MacroBaseConf.DETECTOR_TYPE,
                 MacroBaseConf.DetectorType.MAD);
        assertTrue(new DummyAnalyzer(conf).constructDetector(0L) instanceof MAD);

        conf.set(MacroBaseConf.DETECTOR_TYPE,
                 MacroBaseConf.DetectorType.MCD);
        assertTrue(new DummyAnalyzer(conf).constructDetector(0L) instanceof MinCovDet);

        conf.set(MacroBaseConf.DETECTOR_TYPE,
                 MacroBaseConf.DetectorType.ZSCORE);
        assertTrue(new DummyAnalyzer(conf).constructDetector(0L) instanceof ZScore);

        conf.set(MacroBaseConf.DETECTOR_TYPE,
                 MacroBaseConf.DetectorType.KDE);
        assertTrue(new DummyAnalyzer(conf).constructDetector(0L) instanceof KDE);

        conf.set(MacroBaseConf.DETECTOR_TYPE,
                 MacroBaseConf.DetectorType.BINNED_KDE);
        assertTrue(new DummyAnalyzer(conf).constructDetector(0L) instanceof BinnedKDE);

        conf.set(MacroBaseConf.DETECTOR_TYPE,
                 MacroBaseConf.DetectorType.TREE_KDE);
        assertTrue(new DummyAnalyzer(conf).constructDetector(0L) instanceof TreeKDE);

        conf.set(MacroBaseConf.DETECTOR_TYPE,
                 MacroBaseConf.DetectorType.MOVING_AVERAGE);
        assertTrue(new DummyAnalyzer(conf).constructDetector(0L) instanceof MovingAverage);
    }

}
