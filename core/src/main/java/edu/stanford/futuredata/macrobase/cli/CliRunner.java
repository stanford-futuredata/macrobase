package edu.stanford.futuredata.macrobase.cli;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.contrib.aria.AriaCubePipeline;
import edu.stanford.futuredata.macrobase.pipeline.BasicBatchPipeline;
import edu.stanford.futuredata.macrobase.pipeline.CubePipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import edu.stanford.futuredata.macrobase.pipeline.Pipeline;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a pipeline based on parameters saved in a config file.
 * New custom pipelines can be created by implementing the Pipeline interface and
 * adding a loader to the PipelineMapper.
 *
 * see demo/cli_conf.yaml
 */
public class CliRunner {

    private static Logger log = LoggerFactory.getLogger(CliRunner.class);

    public static void main(String[] args) throws Exception {
        String configFile = args[0];
        PipelineConfig conf = PipelineConfig.fromYamlFile(configFile);
        Pipeline p = loadPipeline(conf);
        Explanation e = p.results();
        log.info("Computed Results");
        System.out.println(e.prettyPrint());
    }

    public static Pipeline loadPipeline(PipelineConfig conf) throws MacrobaseException{
        String pipelineName = conf.get("pipeline");
        switch (pipelineName) {
            case "BasicBatchPipeline": {
                return new BasicBatchPipeline(conf);
            }
            case "CubePipeline": {
                return new CubePipeline(conf);
            }
            case "AriaCubePipeline": {
                return new AriaCubePipeline(conf);
            }
            default: {
                throw new MacrobaseException("Bad Pipeline");
            }
        }
    }
}
