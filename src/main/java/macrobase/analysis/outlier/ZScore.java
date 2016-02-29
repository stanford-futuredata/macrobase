package macrobase.analysis.outlier;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.List;

import com.codahale.metrics.Timer;

import macrobase.MacroBase;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

public class ZScore extends OutlierDetector {
    private double mean;
    private double std;

    private final Timer meanComputation = MacroBase.metrics.timer(name(ZScore.class, "meanComputation"));
    private final Timer stddevComputation = MacroBase.metrics.timer(name(ZScore.class, "stddevComputation"));
    
    public ZScore(MacroBaseConf conf) {
        super(conf);
    }

    @Override
    public void train(List<Datum> data) {
        double sum = 0;

        Timer.Context context = meanComputation.time();
        for (Datum d : data) {
            assert (d.getMetrics().getDimension() == 1);
            sum += d.getMetrics().getEntry(0);
        }
        mean = sum / data.size();
        context.stop();

        context = stddevComputation.time();
        double ss = 0;
        for (Datum d : data) {
            ss += Math.pow(mean - d.getMetrics().getEntry(0), 2);
        }
        std = Math.sqrt(ss / data.size());
        context.stop();
    }

    @Override
    public double score(Datum datum) {
        double point = datum.getMetrics().getEntry(0);
        return Math.abs(point - mean) / std;
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        // z-score is identity since we're literally calculating the z-score
        return zscore;
    }

    @Override
    public ODDetectorType getODDetectorType() {
        return ODDetectorType.ZSCORE;
    }
}
