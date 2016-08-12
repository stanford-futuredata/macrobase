package macrobase.pipeline;

import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.List;

public class MockTransform extends BatchTrainScore {
    public MockTransform(MacroBaseConf conf) throws ConfigurationException {
        super(conf);
    }

    @Override
    public void train(List<Datum> data) {

    }

    @Override
    public double score(Datum datum) {
        return 0;
    }
}
