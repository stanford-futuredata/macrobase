package macrobase.analysis.pipeline.stream;

import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MBStream<T> {
    private List<T> output;

    public void add(T record) { output.add(record); }

    public void add(List<T> records) {
        output.addAll(records);
    }

    public void close() {}

    public MBStream() {
        output = new ArrayList<>();
    }

    public MBStream(List<T> data) {
        output = data;
    }

    public List<T> drain() {
        return drain(-1);
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
