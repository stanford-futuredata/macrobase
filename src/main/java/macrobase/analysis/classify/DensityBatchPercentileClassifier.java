package macrobase.analysis.classify;

import com.google.common.collect.Lists;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.stats.DensityEstimater;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DensityBatchPercentileClassifier extends OutlierClassifier {
    Iterator<OutlierClassificationResult> resultIterator;
    DensityEstimater densityEstimater;

    final double targetPercentile;

    public DensityBatchPercentileClassifier(MacroBaseConf conf, Iterator<Datum> input) throws ConfigurationException {
        this(conf, input, conf.getDouble(MacroBaseConf.TARGET_PERCENTILE, MacroBaseDefaults.TARGET_PERCENTILE));
    }

    public DensityBatchPercentileClassifier(MacroBaseConf conf, Iterator<Datum> input, double percentile) throws ConfigurationException {
        super(conf, input);
        this.targetPercentile = percentile;
        this.densityEstimater = conf.constructDensityEstimator();
    }

    @Override
    public OutlierClassificationResult next() {
        if (resultIterator == null) {
            List<Datum> toClassify = Lists.newArrayList(input);
            densityEstimater.train(toClassify);
            for (Datum d : toClassify) {
                d.setDensity(densityEstimater.density(d));
            }

            // Use reverse sort because we want points with low density (not high distance, e.g. ZScore)
            toClassify.sort((a, b) -> Double.compare(b.getDensity(), b.getDensity()));

            int splitPoint = (int) (toClassify.size() * targetPercentile);
            List<OutlierClassificationResult> classificationResults = new ArrayList<>(toClassify.size());
            for (int i = 0; i < toClassify.size(); ++i) {
                Datum d = toClassify.get(i);
                classificationResults.add(new OutlierClassificationResult(d, i >= splitPoint));
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
