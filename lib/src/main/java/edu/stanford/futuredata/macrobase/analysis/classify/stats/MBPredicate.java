package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import edu.stanford.futuredata.macrobase.util.MacroBaseException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;

public class MBPredicate {
    public enum PredicateType {
        EQUALS("=="), NOT_EQUALS("!="),
        LESS_THAN("<"), GREATER_THAN(">"),
        LEQ("<="), GEQ(">="),
        STR_RANGE("str_range"), NUM_RANGE("num_range");

        private static PredicateType getEnum(final String str) throws MacroBaseException {
            switch (str) {
                case "==":
                    return EQUALS;
                case "!=":
                    return NOT_EQUALS;
                case "<":
                    return LESS_THAN;
                case ">":
                    return GREATER_THAN;
                case "<=":
                    return LEQ;
                case ">=":
                    return GEQ;
                case "str_range":
                    return STR_RANGE;
                case "num_range":
                    return NUM_RANGE;
                default:
                    throw new MacroBaseException(
                            "PredicateClassifier: Predicate string " + str +
                            " not suppported.");
            }
        }

        final String str;
        PredicateType(String str) {
            this.str = str;
        }
    }

    private boolean isStrPredicate;
    private DoublePredicate doublePredicate;
    private Predicate<String> stringPredicate;

    public MBPredicate(
        final String predicateStr, final Object sentinel
    ) throws MacroBaseException {
        PredicateType pt = PredicateType.getEnum(predicateStr);
        if (pt == PredicateType.NUM_RANGE) {
            Map<String, Collection<List<Number>>> sentinelValue =
                    (Map<String, Collection<List<Number>>>) sentinel;
            Collection<List<Number>> outlierSentinel = sentinelValue.get("outlier");
            doublePredicate = new DoubleRangePredicate(outlierSentinel);
            isStrPredicate = false;
        } else if (pt == PredicateType.STR_RANGE) {
            Map<String, Collection<List<String>>> sentinelValue =
                    (Map<String, Collection<List<String>>>) sentinel;
            Collection<List<String>> outlierSentinel = sentinelValue.get("outlier");
            stringPredicate = new RangePredicate<>(outlierSentinel);
            isStrPredicate = true;
        } else if (sentinel instanceof Number) {
            double sentinelValue = ((Number) sentinel).doubleValue();
            doublePredicate = getSimpleDoublePredicate(predicateStr, sentinelValue);
            isStrPredicate = false;
        } else if (sentinel instanceof String) {
            String sentinelValue = (String) sentinel;
            stringPredicate = getSimpleStrPredicate(predicateStr, sentinelValue);
            isStrPredicate = true;
        } else {
            throw new MacroBaseException("Invalid sentinel type");
        }
    }
    public boolean isStrPredicate() {
        return isStrPredicate;
    }
    public Predicate<String> getStringPredicate() {
        return stringPredicate;
    }
    public DoublePredicate getDoublePredicate() {
        return doublePredicate;
    }

    public class DoubleRangePredicate implements DoublePredicate {
        private Collection<List<Number>> ranges;

        public DoubleRangePredicate(
                Collection<List<Number>> ranges
        ) {
            this.ranges = ranges;
        }

        @Override
        public boolean test(double t) {
            for (List<Number> curRange : ranges) {
                if (curRange.size() == 1) {
                    if (t == curRange.get(0).doubleValue()) {
                        return true;
                    }
                } else {
                    if (curRange.get(0).doubleValue() <= t && t < curRange.get(1).doubleValue()) {
                        return true;
                    }
                }
            }
            return false;
        }
    }


    public class RangePredicate<T extends Comparable<T>> implements Predicate<T> {
        private Collection<List<T>> ranges;

        public RangePredicate(
                Collection<List<T>> ranges
        ) {
            this.ranges = ranges;
        }

        @Override
        public boolean test(T t) {
            for (List<T> curRange : ranges) {
                if (curRange.size() == 1) {
                    if (curRange.get(0).equals(t)) {
                        return true;
                    }
                } else {
                    if (curRange.get(0).compareTo(t) <= 0 && t.compareTo(curRange.get(1)) < 0) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     * @return Lambda function corresponding to the ``predicateStr''. The Lambda function takes in a single
     * argument, which will correspond to the value in the metric column. (A closure is created around the ``sentinel''
     * parameter.)
     * @throws MacroBaseException
     */
    public static DoublePredicate getSimpleDoublePredicate(
            final String predicateStr,
            final double sentinel
    ) throws MacroBaseException {
        switch (PredicateType.getEnum(predicateStr)) {
            case EQUALS:
                return (double x) -> x == sentinel;
            case NOT_EQUALS:
                return (double x) -> x != sentinel;
            case LESS_THAN:
                return (double x) -> x < sentinel;
            case GREATER_THAN:
                return (double x) -> x > sentinel;
            case LEQ:
                return (double x) -> x <= sentinel;
            case GEQ:
                return (double x) -> x >= sentinel;
            default:
                throw new MacroBaseException("Invalid predicate type: "+predicateStr);
        }
    }

    /**
     * @return Lambda function corresponding to the ``predicateStr''. The Lambda function takes in a single
     * argument, which will correspond to the value in the metric column. (A closure is created around the ``sentinel''
     * parameter.)
     * @throws MacroBaseException
     */
    public static Predicate<String> getSimpleStrPredicate(
            final String predicateStr,
            final String sentinel
    ) throws MacroBaseException {
        switch (PredicateType.getEnum(predicateStr)) {
            case EQUALS:
                return (String x) -> x.equals(sentinel);
            case NOT_EQUALS:
                return (String x) -> !(x.equals(sentinel));
            case LESS_THAN:
                return (String x) -> x.compareTo(sentinel) < 0;
            case GREATER_THAN:
                return (String x) -> x.compareTo(sentinel) > 0;
            case LEQ:
                return (String x) -> x.compareTo(sentinel) <= 0;
            case GEQ:
                return (String x) -> x.compareTo(sentinel) >= 0;
            default:
                throw new MacroBaseException("Invalid predicate type: "+predicateStr);

        }
    }
}
