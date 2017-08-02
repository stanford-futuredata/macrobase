package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.conf.Config;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;

/**
 * Creates a custom pipeline object based on config file
 */
public class PipelineMapper {
    public static Pipeline loadPipeline(Config conf) throws MacrobaseException {
        String pipelineName = conf.getAs("pipeline");
        switch (pipelineName) {
            case "BatchPipeline":
                return new BatchPipeline(conf);
            case "CubePipeline":
                return new CubePipeline(conf);
            default:
                throw new MacrobaseException("Bad Pipeline Name");
        }
    }
}
