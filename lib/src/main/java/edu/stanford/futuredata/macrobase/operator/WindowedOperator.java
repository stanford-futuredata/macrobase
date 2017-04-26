package edu.stanford.futuredata.macrobase.operator;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.ArrayUtils;

import java.util.*;

/**
 * Wraps an incremental operator to work over rolling windows.
 * Inputs are batched into panes of [start, start+slideLength).
 * slideLength is assumed to be a divisor of the window length so that
 * a window divides neatly into panes: if not the window length is
 * effectively rounded up.
 * @param <O> output type of the operator
 */
public class WindowedOperator<O>
        implements Operator<DataFrame, O> {
    private String timeColumn = "time";
    private double windowLength = 60.0;
    private double slideLength = 10.0;

    private double maxWindowTime;
    private IncrementalOperator<O> op;

    private ArrayList<DataFrame> batchBuffer;

    public WindowedOperator(IncrementalOperator op) {
        this.op = op;
    }
    public WindowedOperator<O> initialize() {
        this.maxWindowTime = 0.0;
        this.batchBuffer = new ArrayList<>();

        int numPanes = (int)Math.ceil(windowLength / slideLength);
        op.setWindowSize(numPanes);
        return this;
    }


    /**
     * Process a small batch of data. Data is buffered until a pane (or multiple)
     * is filled, then the internal operator state is updated with these panes.
     * Minibatches are split to fit into panes of fixed time length.
     * @param input minibatch of data to process
     * @throws Exception
     */
    @Override
    public void process(DataFrame input) throws Exception {
        List<DataFrame> newPanes = addToBuffer(input);
        for (DataFrame pane: newPanes) {
            op.process(pane);
        }
    }

    /**
     * Build a pane from what is already in the buffer, call when no more events will arrive before
     * the next pane interval.
     * @return new effective window end time
     */
    public double flushBuffer() throws Exception {
        DataFrame partialPane = DataFrame.unionAll(batchBuffer);
        maxWindowTime += slideLength;
        op.process(partialPane);
        batchBuffer.clear();
        return maxWindowTime;
    }

    /**
     * Split buffer + input into panes, keeping any leftover rows in the buffer
     * @param input current minibatch to aprocess
     * @return completed panes derived from the buffer and current input
     */
    protected List<DataFrame> addToBuffer(DataFrame input) {
        int n = input.getNumRows();
        if (n == 0) {
            return Collections.emptyList();
        }
        double[] times = input.getDoubleColumnByName(timeColumn);
        double maxInputTime = ArrayUtils.max(times);

        DataFrame restInput = input;
        ArrayList<DataFrame> newPanes = new ArrayList<>(1);

        // Assuming in order arrival, break up incoming batch into panes
        while (maxInputTime >= maxWindowTime + slideLength) {
            // When we fill up a pane, split off the overflow from the current batch
            double nextWindowEnd = maxWindowTime + slideLength;
            DataFrame earlyInput = restInput.filter(timeColumn, (double t) -> t < nextWindowEnd);
            restInput = restInput.filter(timeColumn, (double t) -> t >= nextWindowEnd);
            batchBuffer.add(earlyInput);
            DataFrame newPane = DataFrame.unionAll(batchBuffer);

            // Reset the batch buffer and start collecting for the next pane
            maxWindowTime = nextWindowEnd;
            batchBuffer.clear();
            newPanes.add(newPane);
        }

        // Buffer partially filled pane until it is filled
        batchBuffer.add(restInput);
        return newPanes;
    }

    @Override
    public O getResults() {
        return op.getResults();
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    public void setTimeColumn(String timeColumn) {
        this.timeColumn = timeColumn;
    }

    public double getWindowLength() {
        return windowLength;
    }

    public void setWindowLength(double windowLength) {
        this.windowLength = windowLength;
    }

    public double getSlideLength() {
        return slideLength;
    }

    public void setSlideLength(double slideLength) {
        this.slideLength = slideLength;
    }

    public double getMaxWindowTime() {
        return maxWindowTime;
    }

    public int getBufferSize() {
        return batchBuffer.size();
    }
    public int getNumBufferedRows() {
        int numRows = 0;
        for (DataFrame df : batchBuffer) {
            numRows += df.getNumRows();
        }
        return numRows;
    }
}

