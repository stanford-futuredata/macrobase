package edu.stanford.futuredata.macrobase.operator;

/**
 * Generic interface for operations. Should not modify input.
 * Batch operators should eagerly evaluate as much as possible in the process method,
 * returning cached results in getResults.
 * Streaming operators may split the work differently.
 * @param <I> Input type
 * @param <O> Output type
 */
public interface Operator<I,O> {
    void process(I input) throws Exception;
    O getResults();
}
