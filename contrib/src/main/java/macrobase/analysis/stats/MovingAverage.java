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
public class MovingAverage extends TimeSeriesScore {
    Deque<DatumWithInfo> window = new ArrayDeque<DatumWithInfo>();
    int weightTotal;
    private RealVector windowSum;

    private static class DatumWithInfo {
        private Datum datum;
        private long weight;

        public DatumWithInfo(Datum datum, long weight) {
            this.datum = datum;
            this.weight = weight;
        }

        public Datum getDatum() {
            return datum;
        }

        public long getWeight() {
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
            long weight = newDatum.getTime(timeColumn) - getLatestDatum().getTime(timeColumn);
            if (window.size() == 1) {
                windowSum = new ArrayRealVector(newDatum.metrics()
                        .getDimension());
                // Remove and re-add first datum with the correct weight
                Datum first = window.remove().getDatum();
                // Assume same weight for first as for second
                addDatumWithWeight(first, weight);
            }

            addDatumWithWeight(newDatum, weight);
        }
    }

    private void addDatumWithWeight(Datum d, long weight) {
        windowSum = windowSum.add(d.metrics().mapMultiply(weight));
        weightTotal += weight;
        window.add(new DatumWithInfo(d, weight));
    }

    private Datum getLatestDatum() {
        return window.peekLast().getDatum();
    }

    @Override
    public void removeLastFromWindow() {
        DatumWithInfo head = window.remove();
        long oldWeight = head.getWeight();
        weightTotal -= oldWeight;
        windowSum = windowSum.subtract(head.getDatum().metrics()
                .mapMultiply(oldWeight));
    }

    @Override
    public double scoreWindow() {
        if (windowSum == null) {
            return 0;
        }
        RealVector latest = getLatestDatum().metrics();
        RealVector average = windowSum.mapDivide(weightTotal);
        double percentDiff = 0;
        for (int i = 0; i < average.getDimension(); i++) {
            if (average.getEntry(i) == 0 || timeColumn == i) {
                // What should we do here?
                continue;
            }
            percentDiff += Math.abs((latest.getEntry(i) - average.getEntry(i))
                    / average.getEntry(i));
        }

        return percentDiff;
    }
}
