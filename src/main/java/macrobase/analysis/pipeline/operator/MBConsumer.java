package macrobase.analysis.pipeline.operator;

import java.util.List;

public interface MBConsumer<T> {
    void initialize() throws Exception;
    void consume(List<T> records) throws Exception;
    void shutdown() throws Exception;
}
