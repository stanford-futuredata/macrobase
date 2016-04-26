package macrobase.analysis.classify;

import macrobase.analysis.result.DatumWithNorm;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;

public class BatchingPercentileClassifier implements OutlierClassifier {
    private static final Logger log = LoggerFactory.getLogger(BatchingPercentileClassifier.class);
    protected Iterator<Datum> input;

    // Between 0 - 1
    final double targetPercentile;
    final double cutoff;
    private ArrayList<DatumWithNorm> data;
    private Iterator<DatumWithNorm> iterData = null;

    public BatchingPercentileClassifier(MacroBaseConf conf, Iterator<Datum> input) {
        this(conf, input, conf.getDouble(MacroBaseConf.TARGET_PERCENTILE,
                                         MacroBaseDefaults.TARGET_PERCENTILE));
    }

    public BatchingPercentileClassifier(MacroBaseConf conf, Iterator<Datum> input, double percentile) {
        this.input = input;
        this.targetPercentile = percentile;
        data = new ArrayList<>();

        while(input.hasNext()) {
            data.add(new DatumWithNorm(input.next()));
        }
        double[] scores = new double[data.size()];
        for (int i = 0; i < data.size(); i++) {
            scores[i] = data.get(i).getNorm();
        }

        Percentile pCalc = new Percentile().withNaNStrategy(NaNStrategy.MAXIMAL);
        pCalc.setData(scores);
        this.cutoff = pCalc.evaluate(scores, targetPercentile * 100);
        log.debug("{} Percentile Cutoff: {}", targetPercentile, cutoff);
        log.debug("Median: {}", pCalc.evaluate(50));
        log.debug("Max: {}", pCalc.evaluate(100));
        this.iterData = data.iterator();
    }

    @Override
    public OutlierClassificationResult next() {
        DatumWithNorm d = iterData.next();
        boolean isOutlier = d.getNorm() >= cutoff || d.getNorm().isInfinite();
        return new OutlierClassificationResult(
                d.getDatum(),
                isOutlier
        );
    }

    @Override
    public boolean hasNext() {
        return iterData.hasNext();
    }
}
