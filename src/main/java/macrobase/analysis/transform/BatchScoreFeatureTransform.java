package macrobase.analysis.transform;

import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchScoreFeatureTransform extends BatchTransform {
    protected BatchTrainScore batchTrainScore;
    protected MacroBaseConf conf;

    public BatchScoreFeatureTransform(MacroBaseConf conf, Iterator<Datum> input, MacroBaseConf.TransformType transformType)
            throws ConfigurationException {
        super(input);
        this.batchTrainScore = conf.constructTransform(transformType);
        this.conf = conf;
    }

    public BatchScoreFeatureTransform(MacroBaseConf conf, Iterator<Datum> input) throws ConfigurationException {
        this(conf, input, conf.getTransformType());
    }

    @Override
    protected List<Datum> transform(List<Datum> data) {
        batchTrainScore.train(data);
        List<Datum> results = new ArrayList<>(data.size());
        for(Datum d : data) {
            results.add(new Datum(d, batchTrainScore.score(d)));
        }
        return results;
    }
}
