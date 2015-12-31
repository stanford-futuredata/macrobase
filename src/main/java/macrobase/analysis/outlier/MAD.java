package macrobase.analysis.outlier;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.ArrayList;
import java.util.List;

import macrobase.MacroBase;
import macrobase.datamodel.Datum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;

public class MAD extends OutlierDetector {
    private static final Logger log = LoggerFactory.getLogger(MAD.class);

    private double median;
    private double MAD;

    private final Timer medianComputation = MacroBase.metrics.timer(name(MAD.class, "medianComputation"));
    private final Timer residualComputation = MacroBase.metrics.timer(name(MAD.class, "residualComputation"));
    private final Timer residualMedianComputation = MacroBase.metrics.timer(name(MAD.class, "residualMedianComputation"));

    // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
    private final double MAD_TO_ZSCORE_COEFFICIENT = 1.4826;

    @Override
    public void train(List<Datum> data) {
        Timer.Context context = medianComputation.time();

        assert (data.get(0).getMetrics().getDimension() == 1);
        data.sort((x, y) -> Double.compare(x.getMetrics().getEntry(0),
                                           y.getMetrics().getEntry(0)));

        if (data.size() % 2 == 0) {
            median = (data.get(data.size() / 2 - 1).getMetrics().getEntry(0) +
                      data.get(data.size() / 2 + 1).getMetrics().getEntry(0)) / 2;
        } else {
            median = data.get((int) Math.ceil(data.size() / 2)).getMetrics().getEntry(0);
        }
        context.stop();

        context = residualComputation.time();
        List<Double> residuals = new ArrayList<>(data.size());
        for (Datum d : data) {
            residuals.add(Math.abs(d.getMetrics().getEntry(0) - median));
        }
        context.stop();

        context = residualMedianComputation.time();
        residuals.sort((a, b) -> Double.compare(a, b));

        if (data.size() % 2 == 0) {
            MAD = (residuals.get(data.size() / 2 - 1) +
                   residuals.get(data.size() / 2 + 1)) / 2;
        } else {
            MAD = residuals.get((int) Math.ceil(data.size() / 2));
        }
        context.stop();

        log.trace("trained! median is {}, MAD is {}", median, MAD);
    }

    @Override
    public double score(Datum datum) {
        double point = datum.getMetrics().getEntry(0);
        return Math.abs(point - median) / (MAD);
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        double ret = zscore/MAD_TO_ZSCORE_COEFFICIENT;
        log.trace("setting zscore of {} threshold to {}", zscore, ret);
        return ret;
    }
}
