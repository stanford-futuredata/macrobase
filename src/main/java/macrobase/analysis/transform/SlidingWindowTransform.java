package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.transform.aggregate.BatchedWindowAggregate;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.ConfigurationException;

import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;

import java.util.ArrayList;
import java.util.List;


public class SlidingWindowTransform extends FeatureTransform {
    private final BatchedWindowAggregate windowAggregate;
    private final int timeColumn;
    private final boolean paneEnabled;
    private final int slideSize;
    private int windowSize;
    private int paneSize;
    private int windowStart;
    private int paneStart;
    private int dim = 0;

    private MBStream<Datum> output = new MBStream<>();
    private List<Datum> newSlide = new ArrayList<>();
    private List<Datum> currPane = new ArrayList<>();
    private List<Datum> currWindow = new ArrayList<>();

    private int GCD(int a, int b) { return b==0 ? a : GCD(b, a % b); }

    public SlidingWindowTransform(MacroBaseConf conf, int windowSize)
            throws ConfigurationException  {
        MacroBaseConf.AggregateType aggregateType = conf.getAggregateType();
        this.windowAggregate = conf.constructAggregate(aggregateType);
        this.timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
        this.slideSize = conf.getInt(MacroBaseConf.SLIDE_SIZE, MacroBaseDefaults.SLIDE_SIZE);
        this.windowSize = windowSize;
        this.paneEnabled = windowAggregate.paneEnabled();
        this.paneSize = GCD(windowSize, slideSize);
    }

    private boolean datumInRange(Datum d, int start, int size) {
        return d.getTime(timeColumn) - start < size;
    }

    private boolean canSlideWindow() {
        if (currWindow.isEmpty()) // If this is the first window
            return newSlide.size() == windowSize / paneSize;
        else
            return newSlide.size() == slideSize / paneSize;
    }

    private boolean canFlushWindow(Datum d) {
        return (d.getMetrics().getDimension() != dim || !datumInRange(d, windowStart, windowSize));
    }

    private void flushWindow() {
        if (!currPane.isEmpty())
            produceNewPane();
        // Compute the last window if necessary
        if (!newSlide.isEmpty()) {
            if (paneEnabled)
                // The last window starts from the end of the last pane - window size
                windowStart = newSlide.get(newSlide.size() - 1).getTime(timeColumn) + paneSize - windowSize;
            else
                // Otherwise slide the window by the length of the last slide
                windowStart = newSlide.get(0).getTime(timeColumn) + newSlide.size() - windowSize;
            produceNewWindow(true);
        }
    }

    private List<Datum> slideWindow(boolean flush) {
        List<Datum> expired = new ArrayList<>();
        if (paneEnabled) {
            int slideSize = this.slideSize / paneSize;
            if (flush) {
                slideSize = newSlide.size();
            }
            if (currWindow.size() < slideSize)
                currWindow.clear();
            else if (!currWindow.isEmpty()) {
                expired.addAll(currWindow.subList(0, slideSize));
                currWindow.subList(0, slideSize).clear();
            }
        } else {
            int i = 0;
            while (i < currWindow.size() && datumInRange(currWindow.get(i), windowStart, 0)) { i ++; }
            expired.addAll(currWindow.subList(0, i));
            currWindow.subList(0, i).clear();
        }

        currWindow.addAll(newSlide);

        return expired;
    }

    private void produceNewPane() {
        Datum pane = new Datum(new ArrayList<>(), new ArrayRealVector(dim));
        if (!currPane.isEmpty())
            pane = windowAggregate.aggregate(currPane);
        pane.getMetrics().setEntry(timeColumn, paneStart);
        newSlide.add(pane);
        currPane.clear();
        paneStart += paneSize;
    }

    private void produceNewWindow(boolean flush) {
        List<Datum> expiredSlide = slideWindow(flush);
        Datum newWindow = windowAggregate.updateWindow(newSlide, expiredSlide);
        newWindow.getMetrics().setEntry(timeColumn, windowStart);
        output.add(newWindow);
        newSlide.clear();
        windowStart += this.slideSize;
    }

    private void consumeDatum(List<Datum> records) {
        for (Datum d: records) {
            if (!datumInRange(d, windowStart, windowSize)) {
                produceNewWindow(false);
            }
            newSlide.add(d);
        }
    }

    @Override
    public void consume(List<Datum> records) {
        if (records.isEmpty())
            return;
        if (canFlushWindow(records.get(0))) {
            flushWindow();
            // Start a new window if the new datum does not fit in the current window
            if (!datumInRange(records.get(0), windowStart, windowSize)) {
                currWindow.clear();
                windowAggregate.reset();
                windowStart = records.get(0).getTime(timeColumn);
                paneStart = windowStart;
            }
            dim = records.get(0).getMetrics().getDimension();
        }

        if (!paneEnabled) {
            consumeDatum(records);
        } else {
            for (Datum d: records) {
                while (!datumInRange(d, paneStart, paneSize)) {
                    /* sub aggregate the new pane */
                    produceNewPane();
                    /* if we have enough panes, compute the next window */
                    if (canSlideWindow()) {
                        produceNewWindow(false);
                    }
                }
                currPane.add(d);
            }
        }
    }

    @Override
    public MBStream<Datum> getStream() {
        return output;
    }


    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void shutdown() {
        flushWindow();
    }
}
