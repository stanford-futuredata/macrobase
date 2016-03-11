package macrobase.analysis.outlier;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Counter;

import macrobase.MacroBase;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;

public class MAD extends OutlierDetector {
    private static final Logger log = LoggerFactory.getLogger(MAD.class);

    private double localMedian;
    private double median;
    private double MAD;

    private final Timer medianComputation = MacroBase.metrics.timer(name(MAD.class, "medianComputation"));
    private final Timer residualComputation = MacroBase.metrics.timer(name(MAD.class, "residualComputation"));
    private final Timer residualMedianComputation = MacroBase.metrics.timer(
            name(MAD.class, "residualMedianComputation"));
    private final Counter zeroMADs = MacroBase.metrics.counter(name(MAD.class, "zeroMADs"));

    private final double trimmedMeanFallback = 0.05;
    private List<Datum> bufferedData;

    // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
    private final double MAD_TO_ZSCORE_COEFFICIENT = 1.4826;
    
    public MAD(MacroBaseConf conf) {
        super(conf);
    }

    public double getLocalMedian() { return localMedian; }

    public static double computeMedian(List<Double> array) {
        array.sort((a, b) -> Double.compare(a, b));
        double median;
        if (array.size() % 2 == 0) {
            median = (array.get(array.size() / 2 - 1) +
                      array.get(array.size() / 2)) / 2;
        } else {
            median = array.get((int) Math.ceil(array.size() / 2));
        }
        return median;
    }

    public static double computeMean(List<Double> array) {
        double mean = 0.0;
        for (Double arrayElem : array) {
            mean += arrayElem;
        }
        return mean / array.size();
    }

    @Override
    public void train(List<Datum> data) {
        bufferedData = data;
        Timer.Context context = medianComputation.time();

        assert (data.get(0).getMetrics().getDimension() == 1);
        data.sort((x, y) -> Double.compare(x.getMetrics().getEntry(0),
                                           y.getMetrics().getEntry(0)));

        if (data.size() % 2 == 0) {
            localMedian = (data.get(data.size() / 2 - 1).getMetrics().getEntry(0) +
                      data.get(data.size() / 2).getMetrics().getEntry(0)) / 2;
        } else {
            localMedian = data.get((int) Math.ceil(data.size() / 2)).getMetrics().getEntry(0);
        }
        context.stop();

        log.trace("trained! localMedian is {}, MAD is {}", localMedian, MAD);
    }

    public double getApproximateMAD(double approximateMedian) {
        Timer.Context context = residualComputation.time();
        List<Double> residuals = new ArrayList<>(bufferedData.size());
        for (Datum d : bufferedData) {
            residuals.add(Math.abs(d.getMetrics().getEntry(0) - approximateMedian));
        }
        context.stop();

        context = residualMedianComputation.time();
        double approximateMAD = computeMedian(residuals);

        if (approximateMAD == 0) {
            zeroMADs.inc();
            int lowerTrimmedMeanIndex = (int) (residuals.size() * trimmedMeanFallback);
            int upperTrimmedMeanIndex = (int) (residuals.size() * (1 - trimmedMeanFallback));
            log.trace("MAD was zero; using trimmed means of residuals ({})", trimmedMeanFallback);
            double sum = 0;
            for (int i = lowerTrimmedMeanIndex; i < upperTrimmedMeanIndex; ++i) {
                sum += residuals.get(i);
            }
            approximateMAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
            assert (approximateMAD != 0);
        }

        context.stop();

        return approximateMAD;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    public void setMAD(double MAD) {
        this.MAD = MAD;
    }

    @Override
    public double score(Datum datum) {
        double point = datum.getMetrics().getEntry(0);
        return Math.abs(point - median) / (MAD);
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        double ret = zscore / MAD_TO_ZSCORE_COEFFICIENT;
        log.trace("setting zscore of {} threshold to {}", zscore, ret);
        return ret;
    }

    @Override
    public ODDetectorType getODDetectorType() {
        return ODDetectorType.MAD;
    }
}
