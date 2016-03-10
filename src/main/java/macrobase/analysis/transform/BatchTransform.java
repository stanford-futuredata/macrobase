package macrobase.analysis.transform;

import com.google.common.collect.Lists;
import macrobase.datamodel.Datum;

import java.util.Iterator;
import java.util.List;

public abstract class BatchTransform extends FeatureTransform {
    protected Iterator<Datum> inputIterator;
    protected Iterator<Datum> outputIterator;

    public BatchTransform(Iterator<Datum> inputIterator) {
        this.inputIterator = inputIterator;
    }

    protected abstract List<Datum> transform(List<Datum> data);

    @Override
    public boolean hasNext() {
        return inputIterator.hasNext() || (outputIterator != null && outputIterator.hasNext());
    }

    @Override
    public Datum next() {
        if(outputIterator == null) {
            outputIterator = transform(Lists.newArrayList(inputIterator)).iterator();
        }

        return outputIterator.next();
    }
}
