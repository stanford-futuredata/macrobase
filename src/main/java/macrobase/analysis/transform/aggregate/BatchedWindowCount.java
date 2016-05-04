package macrobase.analysis.transform.aggregate;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;

public class BatchedWindowCount extends BatchedWindowAggregate {
    public BatchedWindowCount() {}

    public BatchedWindowCount(MacroBaseConf conf) {
        this.timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
    }

    public Datum process(List<Datum> new_data, List<Datum> expired_data) {
        /* TODO: Incremental updates for window count */
        return null;
    }

    public Datum process(List<Datum> data) {
        if (data.isEmpty())
            return null;

        if (timeColumn == null) {
            return new Datum(new ArrayList<>(), data.size());
        } else {
            int dim = data.get(0).getMetrics().getDimension();
            RealVector results = new ArrayRealVector(dim);
            for (int i = 0; i < dim; i ++) {
                results.setEntry(i, data.size());
            }
            results.setEntry(timeColumn, data.get(0).getMetrics().getEntry(timeColumn));

            return new Datum(new ArrayList<>(), results);
        }
    }
}
