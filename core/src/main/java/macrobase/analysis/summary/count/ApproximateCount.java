package macrobase.analysis.summary.count;

import java.util.Collection;
import java.util.Map;

/**
 * Created by pbailis on 12/26/15.
 */
public interface ApproximateCount {
    void multiplyAllCounts(Double by);

    Map<Integer, Double> getCounts();

    void observe(Integer item, double count);

    double getTotalCount();

    double getCount(int item);

    default void observe(Collection<Integer> items) {
        for (Integer item : items) {
            observe(item, 1.0);
        }
    }

    default void observe(Integer item) {
        observe(item, 1);
    }
}
