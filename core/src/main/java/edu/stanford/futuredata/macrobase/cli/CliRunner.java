package edu.stanford.futuredata.macrobase.cli;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.pipeline.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a pipeline based on parameters saved in a config file.
 * New custom pipelines can be created by implementing the Pipeline interface and
 * adding a loader to the PipelineMapper.
 *
 * see demo/batch.yaml
 */
public class CliRunner {

    private static Logger log = LoggerFactory.getLogger(CliRunner.class);

    public static void main(String[] args) throws Exception {
        String configFile = args[0];
        PipelineConfig conf = PipelineConfig.fromYamlFile(configFile);
        Pipeline p = PipelineUtils.createPipeline(conf);
        Explanation e = p.results();
        log.info("Computed Results");
        System.out.println(e.prettyPrint());
    }
}
