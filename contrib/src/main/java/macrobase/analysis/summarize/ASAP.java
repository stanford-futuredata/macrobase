package macrobase.analysis.summarize;

import macrobase.analysis.pipeline.stream.TimeDatumStream;
import macrobase.analysis.stats.Autocorrelation;
import macrobase.analysis.summarize.util.ASAPMetrics;
import macrobase.analysis.transform.BatchSlidingWindowTransform;
import macrobase.analysis.transform.aggregate.AggregateConf;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.List;

public class ASAP {
    public List<Datum> currWindow;
    public long binSize;
    public int windowSize = 1;
    public int pointsChecked = 0;

    /* Do not smooth for time series whose initial kurtosis is above this threshold */
    private double KURT_THRESH = 5;
    /* Search for window sizes up to #points / MAX_WINDOW */
    private int MAX_WINDOW = 10;
    /* Kurtosis to preserve after smoothing */
    private double PRESERVATION_THRESH = 1;

    private BatchSlidingWindowTransform swTransform;
    private MacroBaseConf conf = new MacroBaseConf();
    private Autocorrelation acf;
    private ASAPMetrics metrics;
    private TimeDatumStream stream;
    private double minObj;
    private boolean preAggregate;

    public ASAP(TimeDatumStream stream, long windowRange, int resolution, boolean preAggregate) throws Exception {
        this.stream = stream;
        this.preAggregate = preAggregate;
        conf.set(MacroBaseConf.TIME_COLUMN, stream.timeColumn);
        conf.set(AggregateConf.AGGREGATE_TYPE, AggregateConf.AggregateType.AVG);

        currWindow = stream.drainDuration(windowRange);
        // Right now ASAP only supports a timestamp dimension and a metrics dimension
        assert (currWindow.get(0).metrics().getDimension() == 2);
        binSize = getBinSize(windowRange, resolution);
        if (preAggregate) {
            preAggregate();
        } else {
            metrics = new ASAPMetrics(currWindow, 1 - stream.timeColumn);
        }

        acf = new Autocorrelation(currWindow.size() / MAX_WINDOW, 1 - stream.timeColumn);
        // Only search for time series whose initial Kurtosis is not too large
        if (metrics.originalKurtosis < KURT_THRESH) { acf.evaluate(currWindow); }

    }

    public void setKurtThresh(double thresh) { KURT_THRESH = thresh; }

    public void setMaxWindow(int maxWindow) { MAX_WINDOW = maxWindow; }

    public void setPreservationThresh(double thresh) { PRESERVATION_THRESH = thresh; }

    /* Get bin size given time period and resolution to visualize.
    *  Round bin size to the nearest 10min / hour / day. */
    private long getBinSize(long windowRange, int resolution) {
        long binSize = windowRange / resolution;
        long dayInSec = 24 * 3600 * 1000L;
        long hourInSec = 3600 * 1000L;

        if (binSize > dayInSec) { // Round to the nearest day
            binSize = Math.round(binSize * 1.0 / dayInSec) * dayInSec;
        } else if (binSize > hourInSec) {
            binSize = Math.round(binSize * 1.0 / hourInSec) * hourInSec;
        } else if (binSize > 600000) { // Round to the nearest multilples of 10 min
            binSize = Math.round(binSize * 1.0 / 600000) * 600000;
        } else if (binSize > 1000) {
            binSize = (binSize / 1000 + 1) * 1000;
        }
        if (currWindow.size() > 1) {
            long minInterval = currWindow.get(1).getTime(stream.timeColumn) - currWindow.get(0).getTime(stream.timeColumn);
            binSize = Math.max(binSize, minInterval);
        }

        return binSize;
    }

    /* Moving average transform */
    private List<Datum> transform(int w) throws ConfigurationException {
        pointsChecked += 1;
        conf.set(MacroBaseConf.TIME_WINDOW, w * binSize);
        swTransform = new BatchSlidingWindowTransform(conf, binSize);
        swTransform.consume(currWindow);
        swTransform.shutdown();
        return swTransform.getStream().drain();
    }

    /* Pixel-aware preaggregation */
    private void preAggregate() throws ConfigurationException {
        List<Datum> panes = transform(1);
        this.currWindow = panes;
        metrics = new ASAPMetrics(panes, 1 - stream.timeColumn);
    }

    private void checkLastWindow() throws ConfigurationException {
        if (windowSize == 1) { return; }
        // Check window from last frame
        List<Datum> windows = transform(windowSize);
        double[] results = metrics.roughnessKurtosis(windows);
        if (results[1] >= metrics.originalKurtosis) {
            minObj = results[0];
            return;
        } else {
            windowSize = 1;
        }
    }

    /* Check whether the current choice of window size will produce a time series
       rougher than the current optimal */
    private boolean roughnessGreaterThanOpt(int w) {
        return Math.sqrt(1 - acf.correlations[w]) * windowSize > Math.sqrt(1 - acf.correlations[windowSize]) * w;
    }

    /* Update lower bound of the window size to search */
    private int updateLB(int lowerBoundWindow, int w) {
        return (int) Math.round(Math.max(w * Math.sqrt((acf.maxACF - 1) / (acf.correlations[w] - 1)), lowerBoundWindow));
    }

    /* Binary search */
    private void binarySearch(int head, int tail) throws ConfigurationException {
        while (head <= tail) {
            int w = (head + tail) / 2;
            List<Datum> windows = transform(w);
            double kurtosis = metrics.kurtosis(windows);
            if (kurtosis >= PRESERVATION_THRESH * metrics.originalKurtosis) { /* Search second half if feasible */
                double smoothness = metrics.roughness(windows);
                if (smoothness < minObj) {
                    windowSize = w;
                    minObj = smoothness;
                }
                head = w + 1;
            } else { /* Search first half */
                tail = w - 1;
            }
        }
    }

    /* ASAP algorithm */
    public int findWindow() throws ConfigurationException {
        pointsChecked = 0;
        int N = currWindow.size();

        int largestFeasible = -1;
        int tail = N / MAX_WINDOW;
        minObj = Double.MAX_VALUE;
        /* Check feasibility of window size from the last run
           (for speeding up the streaming case) */
        int lowerBoundWindow = 1;
        checkLastWindow();
        if (windowSize > 1) { lowerBoundWindow = updateLB(lowerBoundWindow, windowSize); }

        /* Only proceed to smoothing if initial kurtosis is lower than threshold */
        if (metrics.originalKurtosis < KURT_THRESH) {
            List<Integer> peaks = acf.findPeaks();
            int j = peaks.size() - 1;
            for (int i = j; i >= 0; i --) {
                int w = peaks.get(i);
                /* Pruning */
                if (w == windowSize) {
                    continue;
                } else if (w < lowerBoundWindow || w == 1) {
                    break;
                } else if (roughnessGreaterThanOpt(w)) {
                    continue;
                }
                List<Datum> windows = transform(w);
                double[] results = metrics.roughnessKurtosis(windows);
                if ((results[1] + 3) >= PRESERVATION_THRESH * (metrics.originalKurtosis + 3)) {
                    if (results[0] < minObj) {
                        minObj = results[0];
                        windowSize = w;
                    }
                    if (largestFeasible < 0) { largestFeasible = i; }
                    lowerBoundWindow = updateLB(lowerBoundWindow, w);
                }
            }
            if (largestFeasible >= 0) {
                if (largestFeasible < peaks.size() - 2) { tail = peaks.get(largestFeasible + 1); }
                lowerBoundWindow = Math.max(lowerBoundWindow, peaks.get(largestFeasible) + 1);
            }
        }
        // Binary search from max period to largest permitted window size
        binarySearch(lowerBoundWindow, tail);

        return windowSize;
    }

    /* Exhaustive search */
    public int findWindowExhaustive() throws ConfigurationException {
        pointsChecked = 0;
        minObj = Double.MAX_VALUE;
        for (int w = 2; w < currWindow.size() / MAX_WINDOW; w ++) {
            List<Datum> windows = transform(w);
            double[] results = metrics.roughnessKurtosis(windows);
            if ((results[1] + 3) >= PRESERVATION_THRESH * (metrics.originalKurtosis + 3)) {
                if (results[0] < minObj) {
                    minObj = results[0];
                    windowSize = w;
                }
            }
        }
        return windowSize;
    }

    /* Update points in the current window in the streaming scenario */
    public void updateWindow(long interval) throws ConfigurationException {
        List<Datum> data = stream.drainDuration(interval);
        if (preAggregate) { // Pixel-aware aggregate
            conf.set(MacroBaseConf.TIME_WINDOW, binSize);
            BatchSlidingWindowTransform sw = new BatchSlidingWindowTransform(conf, binSize);
            sw.consume(data);
            sw.shutdown();
            List<Datum> newPanes = sw.getStream().drain();
            currWindow.addAll(newPanes);
            currWindow.subList(0, newPanes.size()).clear();
            metrics.updateKurtosis(currWindow);
            if (metrics.originalKurtosis < KURT_THRESH) {
                acf.evaluate(currWindow);
            }
        } else {
            currWindow.addAll(data);
            currWindow.subList(0, data.size()).clear();
        }
    }
}