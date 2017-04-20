package macrobase.analysis.stats;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import macrobase.MacroBase;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

public class MultiMAD {
    private static final Logger log = LoggerFactory.getLogger(MultiMAD.class);

    private double median;
    private double MAD;
    private int column;

    private final Timer medianComputation = MacroBase.metrics.timer(name(MultiMAD.class, "medianComputation"));
    private final Timer residualComputation = MacroBase.metrics.timer(name(MultiMAD.class, "residualComputation"));
    private final Timer residualMedianComputation = MacroBase.metrics.timer(
            name(MultiMAD.class, "residualMedianComputation"));
    private final Counter zeroMADs = MacroBase.metrics.counter(name(MultiMAD.class, "zeroMADs"));

    private final double trimmedMeanFallback = 0.05;

    // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
    private final double MAD_TO_ZSCORE_COEFFICIENT = 1.4826;
    
    public MultiMAD() {
        this(0);
    }

    public MultiMAD(int column) {
        this.column = column;
    }

    // @Override
    public void train(List<Datum> data) {
        Timer.Context context = medianComputation.time();

        int len = data.size();
        double[] metrics = new double[data.size()];
        for (int i = 0; i < len; i++) {
            metrics[i] = data.get(i).metrics().getEntry(column);
        }

        Arrays.sort(metrics);

        if (len % 2 == 0) {
            median = (metrics[len / 2 - 1] + metrics[len / 2]) / 2;
        } else {
            median = metrics[(int) Math.ceil(len / 2)];
        }
        context.stop();

        context = residualComputation.time();
        double[] residuals = new double[len];
        for (int i = 0; i < len; i++) {
            residuals[i] = Math.abs(metrics[i] - median);
        }
        context.stop();

        context = residualMedianComputation.time();
        Arrays.sort(residuals);

        if (data.size() % 2 == 0) {
            MAD = (residuals[data.size() / 2 - 1] +
                   residuals[data.size() / 2]) / 2;
        } else {
            MAD = residuals[(int) Math.ceil(data.size() / 2)];
        }

        if (MAD == 0) {
            zeroMADs.inc();
            int lowerTrimmedMeanIndex = (int) (residuals.length * trimmedMeanFallback);
            int upperTrimmedMeanIndex = (int) (residuals.length * (1 - trimmedMeanFallback));
            log.trace("MAD was zero; using trimmed means of residuals ({})", trimmedMeanFallback);
            double sum = 0;
            for (int i = lowerTrimmedMeanIndex; i < upperTrimmedMeanIndex; ++i) {
                sum += residuals[i];
            }
            MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
            assert (MAD != 0);
        }

        context.stop();

        log.trace("trained! median is {}, MAD is {}", median, MAD);
    }

    // @Override
    public double score(Datum datum) {
        double point = datum.metrics().getEntry(column);
        return Math.abs(point - median) / (MAD);
    }

    public double getZScoreEquivalent(double zscore) {
        return zscore / MAD_TO_ZSCORE_COEFFICIENT;
    }
}
