package macrobase.analysis.transform;

import macrobase.datamodel.Datum;

import java.util.Iterator;

public abstract class UnaryFeatureTransform extends FeatureTransform {
    protected final Iterator<Datum> inputIterator;

    public UnaryFeatureTransform(Iterator<Datum> inputIterator) {
        this.inputIterator = inputIterator;
    }
}
