package macrobase.ingest;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;


public class CSVIngesterTest {
    private static final Logger log = LoggerFactory.getLogger(CSVIngesterTest.class);

    @Test
    public void testSimple() throws IOException, ConfigurationException {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");
        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A2","A5"));
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, Lists.newArrayList("A1","A3","A4"));
        CSVIngester ingester = new CSVIngester(conf);

        Datum datum = ingester.next();
        assertEquals(43, datum.getMetrics().getEntry(0), 0.0);
        assertEquals(45, datum.getMetrics().getEntry(1), 0.0);

        while(ingester.hasNext()) {
            ingester.next();
        }
    }

    @Test
    /**
     * Test that when we encounter missing metrics we skip the entire row and read
     * all of the valid rows.
     */
    public void testMissingData() throws IOException, ConfigurationException {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/missingdata.csv");
        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("a1"));
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, Lists.newArrayList("m1","m2"));

        CSVIngester ingester = new CSVIngester(conf);

        int count = 0;
        while (ingester.hasNext()) {
            Datum datum = ingester.next();
            if (count == 3) {
                assertEquals(5, datum.getMetrics().getEntry(0), 0.0);
            }
            count++;
        }

        assertEquals(4, count);
    }
}
