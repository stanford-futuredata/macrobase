package edu.stanford.futuredata.macrobase.config;

import edu.stanford.futuredata.macrobase.conf.Config;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfigTest {
    @Test
    public void testLoadFile() throws Exception {
        Config c = Config.loadFromYaml("demo/conf.yaml");
        List<String> attributes = c.getAs("attributes");
        assertTrue(attributes.size() > 1);
        double pct = c.getAs("percentile");
        assertEquals(1.0, pct, 1e-10);
    }
}
