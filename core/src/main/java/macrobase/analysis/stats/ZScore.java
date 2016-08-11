package macrobase.analysis.stats;

import com.codahale.metrics.Timer;
import macrobase.MacroBase;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

public class ZScore extends BatchTrainScore {
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
        return zscore;
    }
}
