package macrobase.analysis.classify;

import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


public class MultiMADBenchmarkTest {
    @Test
    public void simpleTest() {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A1","A2","A3","A4","A5"))
                .set(MacroBaseConf.METRICS, Lists.newArrayList())
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");
        CSVIngester ingester = new CSVIngester(conf);
        List<Datum> data = ingester.getStream().drain();

        long startTime = System.nanoTime();

        MultiMADClassifier mad = new MultiMADClassifier(5);
        mad.initialize();
        mad.consume(data);
        List<OutlierClassificationResult> results = mad.getStream().drain();
        mad.shutdown();

        long estimatedTime = System.nanoTime() - startTime;
        System.out.format("Time elapsed: %d%n", estimatedTime);
    }
}
