package macrobase.analysis.classify;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.analysis.stats.MultiMAD;

import org.apache.commons.math3.special.Erf;

import java.lang.Math;

import java.util.List;

public class MultiMADClassifier extends OutlierClassifier {
    MBStream<OutlierClassificationResult> results = new MBStream<>();

    private double percentile = 0.5;
    private double cutoff = 2.576;

    public MultiMADClassifier() {
        this(0.5);
    }

    public MultiMADClassifier(double percentile) {
        this.percentile = percentile;
        this.cutoff = Math.sqrt(2) * Erf.erfcInv(2.0*percentile/100.0);
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
        boolean[] isOutlier = new boolean[records.size()];

		for (int dim = 0; dim < records.get(0).metrics().getDimension(); dim++) {
			MultiMAD mad = new MultiMAD(dim);
			mad.train(records);

			for (int i = 0; i < records.size(); i++) {
				Datum d = records.get(i);
				double score = mad.getZScoreEquivalent(mad.score(d));
				if (score >= cutoff) {
                    isOutlier[i] = true;
                }
			}
    	}

        for (int i = 0; i < records.size(); i++) {
            results.add(new OutlierClassificationResult(records.get(i), isOutlier[i]));
        }
    }

    @Override
    public void shutdown() {
    }
}
