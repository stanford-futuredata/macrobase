package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.IncrementalOperator;

import java.util.ArrayDeque;
import java.util.Deque;

public class MovingAverage implements IncrementalOperator<Double> {
    private String columnName;
    private int windowSize = 0;
    private Deque<Double> paneSums;
    private Deque<Integer> paneCounts;

    public MovingAverage(String columnName, int windowSize) {
        this.columnName = columnName;
        this.windowSize = windowSize;
        paneSums = new ArrayDeque<>(windowSize+1);
        paneCounts = new ArrayDeque<>(windowSize+1);
    }

    @Override
    public void process(DataFrame input) throws Exception {
        double[] vals = input.getDoubleColumnByName(columnName);
        double sum = 0.0;
        for (double v : vals) {
            sum += v;
        }
        paneCounts.add(vals.length);
        paneSums.add(sum);
        while (paneCounts.size() > windowSize) {
            paneCounts.removeFirst();
            paneSums.removeFirst();
        }
    }

    @Override
    public Double getResults() {
        double totalSum = 0.0;
        long totalCount = 0;
        for (double s : paneSums) {
            totalSum += s;
        }
        for (int c : paneCounts) {
            totalCount += c;
        }
        return totalSum / totalCount;
    }

    @Override
    public void setWindowSize(int numPanes) {
        this.windowSize = numPanes;
    }

    @Override
    public int getWindowSize() {
        return windowSize;
    }
}
