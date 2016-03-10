package macrobase.analysis.transform;

import macrobase.datamodel.Datum;

import java.util.Iterator;

public abstract class FeatureTransform implements Iterator<Datum> {

    @Override
    public abstract boolean hasNext();

    @Override
    public abstract Datum next();
}
