package edu.stanford.futuredata.macrobase.cli;

import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PipelineConfigTest {
    @Test
    public void testLoadFile() throws Exception {
        PipelineConfig c = PipelineConfig.fromYamlFile("demo/batch.yaml");
        List<String> attributes = c.get("attributes");
        assertTrue(attributes.size() > 1);
        double pct = c.get("cutoff");
        assertEquals(1.0, pct, 1e-10);
    }

    @Test
    public void testLoadJson() throws Exception {
        String json = "{\"hello\":[1,2,3]}";
        PipelineConfig c = PipelineConfig.fromJsonString(json);
        List<Integer> o = c.get("hello");
        assertEquals(2, o.get(1).intValue());
    }
}
