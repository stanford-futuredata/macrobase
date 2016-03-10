package macrobase.analysis.classify;

import com.google.common.collect.Lists;
import macrobase.analysis.result.DatumWithNorm;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.IterUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class BatchingPercentileClassifier extends OutlierClassifier {
    Iterator<OutlierClassificationResult> resultIterator;

    final double targetPercentile;

    public BatchingPercentileClassifier(MacroBaseConf conf, Iterator<Datum> input) {
        this(conf, input, conf.getDouble(MacroBaseConf.TARGET_PERCENTILE,
                                         MacroBaseDefaults.TARGET_PERCENTILE));
    }

    public BatchingPercentileClassifier(MacroBaseConf conf, Iterator<Datum> input, double percentile) {
        super(conf, input);
        this.targetPercentile = percentile;
    }

    @Override
    public OutlierClassificationResult next() {
        if(resultIterator == null) {
            List<DatumWithNorm> toClassify = IterUtils.stream(input)
                    .map(d -> new DatumWithNorm(d))
                    .collect(Collectors.toList());

            toClassify.sort((a, b) -> a.getNorm().compareTo(b.getNorm()));
            List<OutlierClassificationResult> classificationResults = new ArrayList<>(toClassify.size());

            int splitPoint = (int) (toClassify.size() * targetPercentile);
            for(int i = 0; i < toClassify.size(); ++i) {
                DatumWithNorm d = toClassify.get(i);
                classificationResults.add(new OutlierClassificationResult(d.getDatum(), i >= splitPoint));
            }

            resultIterator = classificationResults.iterator();
        }

        return resultIterator.next();
    }

    @Override
    public boolean hasNext() {
        return input.hasNext() || (resultIterator != null && resultIterator.hasNext());
    }
}
