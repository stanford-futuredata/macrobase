package macrobase.analysis.transform.aggregate;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;

public class BatchedWindowMax extends BatchedWindowAggregate {
    public BatchedWindowMax() {}

    public BatchedWindowMax(MacroBaseConf conf) {
        this.timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
    }

    public Datum process(List<Datum> new_data, List<Datum> expired_data) {
        /* TODO: Incremental updates for window max */
        return null;
    }

    public Datum process(List<Datum> data) {
        if (data.isEmpty())
            return null;

        int dim = data.get(0).getMetrics().getDimension();
        RealVector results = new ArrayRealVector(dim);
        results.set(Double.MIN_VALUE);
        /* Set time column of aggregate result to be the start time of the window. */
        if (timeColumn != null)
            results.setEntry(timeColumn, data.get(0).getMetrics().getEntry(timeColumn));

        for (Datum d : data) {
            RealVector metrics = d.getMetrics();
            for (int i = 0; i < dim; i ++) {
                if (timeColumn != null && i == timeColumn)
                    continue;
                results.setEntry(i, Math.max(results.getEntry(i), metrics.getEntry(i)));
            }
        }

        return new Datum(new ArrayList<>(), results);
    }
}
