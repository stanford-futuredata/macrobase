package macrobase.analysis.classify;

import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.Iterator;

/**
 * Consumes a tuple at-a-time from a FeatureTransform and for each tuple outputs
 * a OutlierClassificationResult that should indicate if the tuple observed was an
 * outlier or not.
 */
public abstract class OutlierClassifier implements Iterator<OutlierClassificationResult> {

    protected Iterator<Datum> input;

    public OutlierClassifier(MacroBaseConf conf, Iterator<Datum> input) {
        this.input = input;
    }

    @Override
    public abstract boolean hasNext();

    @Override
    public abstract OutlierClassificationResult next();
}
