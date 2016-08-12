package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class BatchTrainScore {
    private static final Logger log = LoggerFactory.getLogger(BatchTrainScore.class);

    // Constructor with this signature should be implemented by subclasses
    public BatchTrainScore(MacroBaseConf conf) {}

    public abstract void train(List<Datum> data);

    public abstract double score(Datum datum);

}
