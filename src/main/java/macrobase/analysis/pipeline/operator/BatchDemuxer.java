package macrobase.analysis.pipeline.operator;

import com.google.common.collect.Iterators;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchDemuxer implements Iterator<Datum> {
    private final Iterator<Datum> output;

    public BatchDemuxer(Iterator<Iterator<Datum>> inputs) {
        List<Datum> outputList = new ArrayList<>();
        inputs.forEachRemaining(i -> Iterators.addAll(outputList, i));
        output = outputList.iterator();
    }

    @Override
    public boolean hasNext() {
        return output.hasNext();
    }

    @Override
    public Datum next() {
        return output.next();
    }
}
