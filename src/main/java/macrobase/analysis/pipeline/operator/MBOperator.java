package macrobase.analysis.pipeline.operator;

import macrobase.analysis.pipeline.stream.MBStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class MBOperator<S, T> implements MBConsumer<S>, MBProducer<T> {
    private static final Logger log = LoggerFactory.getLogger(MBOperator.class);

    public <Y> MBOperator<S, Y> then(MBOperator<T, Y> o2, int batchSize) {
        MBOperator<S, T> self = this;
        return new MBOperator<S, Y>() {
            @Override
            public MBStream<Y> getStream() throws Exception {
                return o2.getStream();
            }

            @Override
            public void initialize() throws Exception {
                self.initialize();
                o2.initialize();
            }

            @Override
            public void consume(List<S> records) throws Exception {
                self.consume(records);
                o2.consume(self.getStream().drain(batchSize));
            }

            @Override
            public void shutdown() throws Exception {
                self.shutdown();
                o2.shutdown();
            }
        };
    }

    public <Y> MBOperator<S, Y> then(MBOperator<T, Y> o2) {
        return then(o2, -1);
    }
}