package macrobase.analysis.pipeline.operator;

import macrobase.analysis.pipeline.stream.MBStream;

public interface MBProducer<T> {
    MBStream<T> getStream() throws Exception;
}
