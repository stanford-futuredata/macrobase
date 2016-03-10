package macrobase.ingest;

import com.google.common.collect.Lists;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

public class CSVIngesterTest {
    private static final Logger log = LoggerFactory.getLogger(CSVIngesterTest.class);

    public void simpleTest() throws IOException, ConfigurationException {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE,"src/test/resources/data/simple.csv");
        conf.set(MacroBaseConf.ATTRIBUTES,Lists.newArrayList("A2","A5"));
        conf.set(MacroBaseConf.LOW_METRICS,new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, Lists.newArrayList("A1","A3","A4"));
        CSVIngester ingester = new CSVIngester(conf);
        for (int i = 0; i < 10; i++) {
            log.debug("%s", ingester.next());
        }
    }
}
