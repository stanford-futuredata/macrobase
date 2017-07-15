package macrobase.ingest;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import com.google.common.collect.Lists;
import macrobase.ingest.result.RowSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CSVIngesterTest {
    private static final Logger log = LoggerFactory.getLogger(CSVIngesterTest.class);

    @Test
    public void testSimple() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");
        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A2","A5"));
        conf.set(MacroBaseConf.METRICS, Lists.newArrayList("A1","A3","A4"));
        CSVIngester ingester = new CSVIngester(conf);

        List<Datum> data = ingester.getStream().drain();

        Datum datum = data.get(0);
        assertEquals(43, datum.metrics().getEntry(0), 0.0);
        assertEquals(45, datum.metrics().getEntry(1), 0.0);
    }

    @Test
    /**
     * Test that when we encounter missing metrics we skip the entire row and read
     * all of the valid rows.
     */
    public void testMissingData() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/missingdata.csv");
        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("a1"));
        conf.set(MacroBaseConf.METRICS, Lists.newArrayList("m1","m2"));

        CSVIngester ingester = new CSVIngester(conf);
        List<Datum> data = ingester.getStream().drain();

        int count = 0;
        for(Datum d : data) {
            if (count == 3) {
                assertEquals(5, d.metrics().getEntry(0), 0.0);
            }
            count++;
        }

        assertEquals(4, count);
    }


    @Test
    public void testGetRows() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");
        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A2","A5"));
        conf.set(MacroBaseConf.METRICS, Lists.newArrayList("A1","A3","A4"));
        conf.set(MacroBaseConf.BASE_QUERY, "csv:///src/test/resources/data/simple.csv");

        CSVIngester ingester = new CSVIngester(conf);

        Map<String, String> preds = new HashMap<>();
        preds.put("A2", "1");

        RowSet rs = ingester.getRows(conf.getString(MacroBaseConf.BASE_QUERY),
                preds,
                10,
                0);

        assertEquals(1, rs.getRows().size());
        System.out.println(rs);
    }
}
