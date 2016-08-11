package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.transform.aggregate.BatchWindowAggregate;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import java.util.List;

public class BatchSWTransform extends SlidingWindowTransform {
    private BatchWindowAggregate windowAggregate;

    public BatchSWTransform(MacroBaseConf conf, int slideSize) throws ConfigurationException {
        MacroBaseConf.AggregateType aggregateType = conf.getAggregateType();
        this.windowAggregate = conf.constructBatchAggregate(aggregateType);
        this.timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
        this.slideSize = slideSize;
        this.windowSize = conf.getInt(MacroBaseConf.WINDOW_SIZE, MacroBaseDefaults.WINDOW_SIZE);
    }

    private void slideWindow() {
        int i = 0;
        while (i < currWindow.size() && datumInRange(currWindow.get(i), windowStart, 0)) { i++; }
        currWindow.subList(0, i).clear();
    }

    private void aggregateWindow() {
        slideWindow();
        Datum newWindow = windowAggregate.aggregate(currWindow);
        newWindow.getMetrics().setEntry(timeColumn, windowStart);
        output.add(newWindow);
        windowStart += this.slideSize;
    }

    @Override
    public void consume(List<Datum> data) {
        if (data.isEmpty())
            return;
        if (windowStart < 0)
            windowStart = data.get(0).getTime(timeColumn);

        for (Datum d: data) {
            while (!datumInRange(d, windowStart, windowSize)) {
                aggregateWindow();
            }
            currWindow.add(d);
        }
    }

    @Override
    public MBStream<Datum> getStream() { return output; }

    @Override
    public void initialize() throws Exception {}

    @Override
    public void shutdown() { aggregateWindow(); }
}
