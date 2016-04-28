package macrobase.analysis.pipeline.operator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MBStream<T> {
    private List<T> output = new ArrayList<>();

    public void add(T record) { output.add(record); }

    public void add(List<T> records) {
        output.addAll(records);
    }

    public void close() {}

    public List<T> drain() {
        List<T> ret = output;
        output = new ArrayList<>();
        return ret;
    }

    public List<T> drain(int maxElements) {
        List<T> ret;

        if(maxElements < 0 || output.size() <= maxElements) {
            ret = output;
            output = new ArrayList<>();
        } else {
            ret = output.subList(0, maxElements);
            output = output.subList(maxElements, output.size());
        }

        return ret;
    }

    public Integer remaining() {
        return output.size();
    }
}
