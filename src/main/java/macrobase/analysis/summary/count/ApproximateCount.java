package macrobase.analysis.summary.count;

import java.util.Collection;
import java.util.Map;

/**
 * Created by pbailis on 12/26/15.
 */
public abstract class ApproximateCount {
    abstract public void multiplyAllCounts(Double by);

    abstract public Map<Integer, Double> getCounts();

    abstract public void observe(Integer item, double count);

    abstract public double getTotalCount();

    abstract public double getCount(int item);

    public void observe(Collection<Integer> items) {
        for (Integer item : items) {
            observe(item, 1.0);
        }
    }

    public void observe(Integer item) {
        observe(item, 1);
    }
}