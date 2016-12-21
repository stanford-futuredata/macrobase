package macrobase.analysis.summarize;

import macrobase.analysis.pipeline.stream.TimeDatumStream;
import macrobase.analysis.transform.aggregate.AggregateConf;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ASAPTest {
    private static List<Datum> data;
    private static MacroBaseConf conf = new MacroBaseConf();

    @BeforeClass
    public static void createTimeDatumStream() throws Exception {
        conf.set(MacroBaseConf.CSV_INPUT_FILE, String.format("src/test/resources/data/nyc_taxi.csv"));
        conf.set(MacroBaseConf.ATTRIBUTES, new ArrayList<>());
        conf.set(MacroBaseConf.METRICS, new ArrayList<>(Arrays.asList("value", "unix_time")));
        conf.set(MacroBaseConf.TIME_COLUMN, 1);
        conf.set(AggregateConf.AGGREGATE_TYPE, AggregateConf.AggregateType.AVG);

        CSVIngester ingester = new CSVIngester(conf);
        data = ingester.getStream().drain();
    }

    @Test
    public void testPreaggregation() throws Exception {
        TimeDatumStream stream = new TimeDatumStream(new ArrayList<>(data), 1);
        ASAP asap = new ASAP(stream, 150 * 24 * 3600 * 1000L, 1200, false);
        assert(asap.currWindow.size() == 7200);
        stream = new TimeDatumStream(new ArrayList<>(data), 1);
        asap = new ASAP(stream, 150 * 24 * 3600 * 1000L, 1200, true);
        assert(asap.currWindow.size() == 1200);
    }

    @Test
    public void testBatch() throws Exception {
        TimeDatumStream stream = new TimeDatumStream(new ArrayList<>(data), 1);
        ASAP asap = new ASAP(stream, 150 * 24 * 3600 * 1000L, 1200, true);
        /* Exhaustive search */
        int windowSize = asap.findWindowExhaustive();
        assert(asap.pointsChecked == 118);
        /* ASAP */
        asap.windowSize = 1;
        assert(asap.findWindow() == windowSize);
        assert(asap.pointsChecked == 5);
    }

    @Test
    public void testStream() throws Exception {
        TimeDatumStream stream = new TimeDatumStream(new ArrayList<>(data), 1);
        ASAP asap = new ASAP(stream, 90 * 24 * 3600 * 1000L, 1200, true);
        while (stream.remaining() > 0) {
            int windowSize = asap.findWindow();
            assert(asap.pointsChecked < 10);
            assert(windowSize == asap.findWindowExhaustive());
            asap.updateWindow(15 * 24 * 3600 * 1000L);
        }

    }

}
