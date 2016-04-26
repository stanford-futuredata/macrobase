package macrobase.analysis.pipeline.operator;

public interface MBProducer<T> {
    public MBStream<T> getStream() throws Exception;
}
