package macrobase.analysis.pipeline.operator;

import java.util.List;

public class MBOperatorChain<F, T> {
    private MBOperatorChain<?, F> parent;

    private MBOperator<F, T> operator;

    private MBStream<T> output = new MBStream<>();

    // create a new chain with this producer at the head...
    public static <T> MBOperatorChain<?, T> begin(MBProducer<T> head) throws Exception{
        return new MBOperatorChain(head.getStream());
    }

    // create a new chain with this list as input...
    public static <T> MBOperatorChain begin(List<T> feed) throws Exception {
        return new MBOperatorChain(feed);
    }

    private MBOperatorChain(List<T> feed) {
        this.output.add(feed);
    }

    private MBOperatorChain(MBStream<T> head) {
        this.output = head;
    }

    private MBOperatorChain(MBOperatorChain<?, F> parent,
                            MBOperator<F, T> operator) throws Exception {
        this.parent = parent;
        this.operator = operator;
        this.output = operator.getStream();
    }

    // executeMiniBatch this operator after the current in the chain...
    public MBOperatorChain then(MBOperator<F, T> cur) throws Exception {
        return new MBOperatorChain(this, cur);
    }

    public MBStream<T> execute() throws Exception {
        return execute(-1);
    }

    // execute the chain
    private MBStream<T> execute(Integer batchSize) throws Exception {
        if(parent != null) {
            operator.consume(parent.execute(batchSize).drain(batchSize));
        }

        return output;
    }

    // execute in mini-batch mode, with batchSize tuples per iteration
    // caller must repeatedly invoke this function...
    public MBStream<T> executeMiniBatch(int batchSize) throws Exception {
        return execute(batchSize);
    }

    public MBStream<T> executeMiniBatchUntilFixpoint(int batchSize) throws Exception {
        int prevRemaining = -1;

        MBStream<T> ret = execute(batchSize);

        while(ret.remaining() != prevRemaining) {
            prevRemaining = ret.remaining();
            ret = execute(batchSize);
        }

        return ret;
    }
}