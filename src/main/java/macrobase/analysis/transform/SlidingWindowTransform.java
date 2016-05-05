package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.transform.aggregate.BatchedWindowAggregate;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.ConfigurationException;

import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;


public class SlidingWindowTransform extends FeatureTransform {
    public int minPaneSize;
    public BatchedWindowAggregate windowAggregate;
    public int timeColumn;

    private final MBStream<Datum> output = new MBStream<>();

    public SlidingWindowTransform(MacroBaseConf conf, int minPaneSize)
            throws ConfigurationException  {
        MacroBaseConf.AggregateType aggregateType = conf.getAggregateType();
        this.windowAggregate = conf.constructAggregate(aggregateType);
        this.timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
        this.minPaneSize = minPaneSize;
    }

    @Override
    public void consume(List<Datum> records) {
        if (records.isEmpty())
            return;

        int pane_start = records.get(0).getTime(timeColumn);
        List<Datum> pane = new ArrayList<Datum>();
        for (Datum d: records) {
            int time_now = d.getTime(timeColumn);
            if (time_now - pane_start >= minPaneSize) {
                output.add(windowAggregate.process(pane));
                pane_start = time_now;
                pane.clear();
            }
            pane.add(d);
        }
        // Add the last pane
        if (!pane.isEmpty())
            output.add(windowAggregate.process(pane));
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

    }
}

