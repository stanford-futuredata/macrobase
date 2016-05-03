package macrobase.analysis.pipeline.stream;

import java.util.ArrayList;
import java.util.List;

public class MBMultiInputStream<T> extends MBStream<T> {
    private final List<MBStream<T>> inputs;
    private int idx = 0;

    public MBMultiInputStream(List<MBStream<T>> inputs) {
        this.inputs = inputs;
    }

    public MBMultiInputStream() {
        this(new ArrayList<>());
    }

    public void addStream(MBStream<T> input) {
        inputs.add(input);
    }

    @Override
    public List<T> drain(int maxElements) {
        List<T> ret = new ArrayList<>();

        int numDrains = 0;
        // loop through once
        while (numDrains < inputs.size() &&
               // until we receive the requested number of elements
               (ret.size() < maxElements
                // or we want to drain all of the elements
                || maxElements < 0)) {
            ret.addAll(inputs.get(idx % inputs.size())
                               .drain(maxElements - ret.size()));
            numDrains++;
            idx++;
        }

        return ret;
    }
}
