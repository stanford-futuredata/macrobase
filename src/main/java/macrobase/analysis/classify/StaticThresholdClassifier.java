package macrobase.analysis.classify;

import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import java.util.Iterator;

public class StaticThresholdClassifier extends OutlierClassifier {

    private Double threshold;
    private Double thresholdSquared;

    public StaticThresholdClassifier(MacroBaseConf conf, Iterator<Datum> input) {
        super(conf, input);
        threshold = conf.getDouble(MacroBaseConf.OUTLIER_STATIC_THRESHOLD, MacroBaseDefaults.OUTLIER_STATIC_THRESHOLD);
        thresholdSquared = threshold * threshold;
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public OutlierClassificationResult next() {
        Datum nextInput = input.next();
        return new OutlierClassificationResult(nextInput, thresholdSquared > nextInput.getMetrics().getNorm());
    }
}
