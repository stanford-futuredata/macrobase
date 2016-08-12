package macrobase.analysis.classify;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BatchingPercentileClassifier extends OutlierClassifier {
    private static final Logger log = LoggerFactory.getLogger(BatchingPercentileClassifier.class);

    MBStream<OutlierClassificationResult> results = new MBStream<>();

    // Between 0 - 1
    final double targetPercentile;

    public BatchingPercentileClassifier(MacroBaseConf conf) {
        this(conf.getDouble(MacroBaseConf.TARGET_PERCENTILE, MacroBaseDefaults.TARGET_PERCENTILE));
    }

    public BatchingPercentileClassifier(double percentile) {
        this.targetPercentile = percentile;
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
        List<DatumWithNorm> toClassify = new ArrayList<>();

        double[] scores = new double[records.size()];
        for(int i = 0; i < records.size(); i++) {
            Datum d = records.get(i);
            DatumWithNorm dwn = new DatumWithNorm(d);
            toClassify.add(dwn);
            scores[i] = dwn.getNorm();
        }

        Percentile pCalc = new Percentile().withNaNStrategy(NaNStrategy.MAXIMAL);
        pCalc.setData(scores);
        double cutoff = pCalc.evaluate(scores, targetPercentile * 100);
        log.debug("{} Percentile Cutoff: {}", targetPercentile, cutoff);
        log.debug("Median: {}", pCalc.evaluate(50));
        log.debug("Max: {}", pCalc.evaluate(100));

        for(DatumWithNorm dwn : toClassify) {
            results.add(new OutlierClassificationResult(dwn.getDatum(),
                                                        dwn.getNorm() >= cutoff || dwn.getNorm().isInfinite()));
        }
    }

    @Override
    public void shutdown() {
    }
}
