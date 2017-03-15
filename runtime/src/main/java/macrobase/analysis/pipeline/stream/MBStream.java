package macrobase.analysis.pipeline.stream;

import com.google.common.collect.Lists;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MBStream<T> {
    protected List<T> output;

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
            List<T> remove = output.subList(0, maxElements);
            ret = Lists.newArrayList(remove);
            remove.clear();
        }

        return ret;
    }

    public Integer remaining() {
        return output.size();
    }
}
