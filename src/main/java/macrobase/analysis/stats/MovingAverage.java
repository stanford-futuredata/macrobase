package macrobase.analysis.stats;

import java.util.Deque;
import java.util.ArrayDeque;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

/*
 * Simple moving average.
 */
public class MovingAverage extends TimeSeriesOutlierDetector {
    Deque<DatumWithInfo> window = new ArrayDeque<DatumWithInfo>();
    int weightTotal;
    private RealVector windowSum;

    private static class DatumWithInfo {
        private Datum datum;
        private int weight;

        public DatumWithInfo(Datum datum, int weight) {
            this.datum = datum;
            this.weight = weight;
        }

        public Datum getDatum() {
            return datum;
        }

        public int getWeight() {
            return weight;
        }
    }

    public MovingAverage(MacroBaseConf conf) {
        super(conf);
    }

    @Override
    public void addToWindow(Datum newDatum) {
        if (window.size() == 0) {
            // We don't know what weight to use for the first datum, so we wait
            // until we have the second one to actually process it.
            window.add(new DatumWithInfo(newDatum, 0));
        } else {
            int weight = newDatum.getTime() - getLatestDatum().getTime();
            if (window.size() == 1) {
                windowSum = new ArrayRealVector(newDatum.getMetrics()
                        .getDimension());
                // Remove and re-add first datum with the correct weight
                Datum first = window.remove().getDatum();
                // Assume same weight for first as for second
                addDatumWithWeight(first, weight);
            }

            addDatumWithWeight(newDatum, weight);
        }
    }

    private void addDatumWithWeight(Datum d, int weight) {
        windowSum = windowSum.add(d.getMetrics().mapMultiply(weight));
        weightTotal += weight;
        window.add(new DatumWithInfo(d, weight));
    }

    private Datum getLatestDatum() {
        return window.peekLast().getDatum();
    }

    @Override
    public void removeLastFromWindow() {
        DatumWithInfo head = window.remove();
        int oldWeight = head.getWeight();
        weightTotal -= oldWeight;
        windowSum = windowSum.subtract(head.getDatum().getMetrics()
                .mapMultiply(oldWeight));
    }

    @Override
    public double scoreWindow() {
        if (windowSum == null) {
            return 0;
        }
        RealVector latest = getLatestDatum().getMetrics();
        RealVector average = windowSum.mapDivide(weightTotal);
        double percentDiff = 0;
        for (int i = 0; i < average.getDimension(); i++) {
            if (average.getEntry(i) == 0) {
                // What should we do here?
                continue;
            }
            percentDiff += Math.abs((latest.getEntry(i) - average.getEntry(i))
                    / average.getEntry(i));
        }

        return percentDiff;
    }
}
