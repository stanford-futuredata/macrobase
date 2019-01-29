package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.cli.PipelineConfigTest;
import spark.utils.IOUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;

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

    @Test
    public void testInlineCsv() throws Exception{

        FileInputStream inputStream = new FileInputStream("demo/sample_inlinecsv.json");
        String jsonString = IOUtils.toString(inputStream);
        PipelineConfig conf = PipelineConfig.fromJsonString(jsonString);
        CubePipeline p = new CubePipeline(conf);
        Explanation e = p.results();
        assertEquals(2328375, e.numTotal(), 1e-10);
    }
}
