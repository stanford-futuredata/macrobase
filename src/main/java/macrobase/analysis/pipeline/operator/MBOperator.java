package macrobase.analysis.pipeline.operator;

public interface MBOperator<S, T> extends MBConsumer<S>, MBProducer<T> { }
