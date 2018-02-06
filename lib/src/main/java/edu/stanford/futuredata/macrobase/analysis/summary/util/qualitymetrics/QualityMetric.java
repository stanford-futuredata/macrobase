package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * Measures how interesting a subgroup is as a function of its linear aggregates.
 * Risk ratio, support, and deviation from mean are examples.
 */
public interface QualityMetric {
    String name();
    QualityMetric initialize(double[] globalAggregates);
    double value(double[] aggregates);
    boolean isMonotonic();

    enum Action {
        KEEP(2),
        NEXT(1),
        PRUNE(0);

        private int val;

        Action(int val) {
            this.val = val;
        }

        public static Action combine(Action a, Action b) {
            if (a.val <= b.val) {
                return a;
            } else {
                return b;
            }
        }
    }

    // can override for more fancy tight quality metric bounds
    default double maxSubgroupValue(double[] aggregates) {
        if (isMonotonic()) {
            return value(aggregates);
        } else {
            return Double.POSITIVE_INFINITY;
        }
    }

    default Action getAction(double[] aggregates, double threshold) {
        if (isPastThreshold(aggregates, threshold)) {
            return Action.KEEP;
        } else if (canPassThreshold(aggregates, threshold)) {
            return Action.NEXT;
        } else {
            return Action.PRUNE;
        }
    }

    default boolean isPastThreshold(double[] aggregates, double threshold) {
        return value(aggregates) >= threshold;
    }

    default boolean canPassThreshold(double[] aggregates, double threshold) {
        return maxSubgroupValue(aggregates) >= threshold;
    }
}
