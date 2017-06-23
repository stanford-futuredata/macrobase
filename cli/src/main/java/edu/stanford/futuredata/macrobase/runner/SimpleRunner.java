package edu.stanford.futuredata.macrobase.runner;

import edu.stanford.futuredata.macrobase.conf.Config;
import edu.stanford.futuredata.macrobase.pipeline.*;

/**
 * Runs a pipeline based on parameters saved in a config file.
 * New custom pipelines can be created by implementing the Pipeline interface and
 * adding a loader to the PipelineMapper.
 *
 * see demo/conf.yaml
 */
public class SimpleRunner {
    public static void main(String[] args) throws Exception {
        String configFile = args[0];
        Config conf = Config.loadFromYaml(configFile);
        Pipeline p = PipelineMapper.loadPipeline(conf);
        p.run();
    }
}
