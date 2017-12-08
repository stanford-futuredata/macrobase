package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BasicBatchPipelineTest {
    @Test
    public void testDemoQuery() throws Exception {
        PipelineConfig conf = PipelineConfig.fromYamlFile(
                "src/test/resources/tiny_conf.yaml"
        );
        BasicBatchPipeline p = new BasicBatchPipeline(conf);
        Explanation e = p.results();
        assertEquals(3.0, e.numTotal(), 1e-10);
    }
}
