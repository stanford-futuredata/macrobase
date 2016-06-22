package macrobase.analysis.classify;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class WeirdTransitionDetector extends OutlierClassifier {
    MBStream<OutlierClassificationResult> results = new MBStream<>();

    private HashSet<Integer> badTransitions;

    public WeirdTransitionDetector(MacroBaseConf conf) {
        Integer[] badTransitionsArray = {13, 14, 16, 23, 24, 26, 31, 32, 41, 42, 61, 62};
        badTransitions = new HashSet<>();
        badTransitions.addAll(Arrays.asList(badTransitionsArray));
        Integer[] suspiciousTransitionsArray = {25, 52, 45, 54, 56, 65};
        badTransitions.addAll(Arrays.asList(suspiciousTransitionsArray));
    }

    @Override
    public MBStream<OutlierClassificationResult> getStream() {
        return results;
    }

    @Override
    public void initialize() {

    }

    @Override
    public void consume(List<Datum> records) {
        for (Datum r : records) {
            results.add(new OutlierClassificationResult(r, badTransitions.contains(
                    (int) Math.round(r.getMetrics().getEntry(0)))));
        }
    }

    @Override
    public void shutdown() {

    }
}