package macrobase.analysis.transform.aggregate;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateConf {
    public static final String AGGREGATE_TYPE = "macrobase.analysis.aggregateType";
    public static final AggregateType AGGREGATE_TYPE_DEFAULT = AggregateConf.AggregateType.COUNT;
    private static final Logger log = LoggerFactory.getLogger(AggregateConf.class);

    public enum AggregateType {
        COUNT,
        SUM,
        MAX,
        AVG
    }

    public static AggregateType getAggregateType(MacroBaseConf conf) throws ConfigurationException {
        if (!conf.isSet(AGGREGATE_TYPE)) {
            return AGGREGATE_TYPE_DEFAULT;
        }
        return AggregateConf.AggregateType.valueOf(conf.getString(AGGREGATE_TYPE));
    }

    public static BatchWindowAggregate constructBatchAggregate(MacroBaseConf conf,
                                                               AggregateType aggregateType)
            throws ConfigurationException {
        switch (aggregateType) {
            case MAX:
                log.info("Using MAX aggregation.");
                return new BatchWindowMax(conf);
            case AVG:
                log.info("Using AVG aggregation.");
                return new BatchWindowAvg(conf);
            default:
                throw new RuntimeException("Unhandled batch aggreation type!" + aggregateType);
        }
    }

    public static IncrementalWindowAggregate constructIncrementalAggregate(MacroBaseConf conf,
                                                                           AggregateType aggregateType)
            throws ConfigurationException {
        switch (aggregateType) {
            case SUM:
                log.info("Using SUM aggregation.");
                return new IncrementalWindowSum(conf);
            case COUNT:
                log.info("Using COUNT aggregation.");
                return new IncrementalWindowCount(conf);
            default:
                throw new RuntimeException("Unhandled incremental aggreation type!" + aggregateType);
        }
    }
}
