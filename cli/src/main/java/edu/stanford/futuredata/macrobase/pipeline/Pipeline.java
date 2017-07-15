package edu.stanford.futuredata.macrobase.pipeline;

/**
 * Configuration and operator control flow should be encapsulated within pipelines.
 * Operators in macrobase-lib are meant to be usable from any context,
 * so it is up to each pipeline to decide how to link them together.
 */
public interface Pipeline {
    void run() throws Exception;
}

