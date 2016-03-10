package macrobase.analysis.transform;

import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.Periodic;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EWFeatureTransform extends FeatureTransform {
    private final ExponentiallyBiasedAChao<Datum> reservoir;
    private final BatchTrainScore scorer;
    private final Iterator<Datum> input;

    private final Periodic retrainer;
    private final Periodic decayer;

    List<Datum> warmupOutput = new ArrayList<>();

    public EWFeatureTransform(MacroBaseConf conf,
                              Iterator<Datum> input) throws ConfigurationException {
        this(conf,
             input,
             conf.getTransformType(),
             conf.getInt(MacroBaseConf.WARMUP_COUNT, MacroBaseDefaults.WARMUP_COUNT),
             conf.getInt(MacroBaseConf.INPUT_RESERVOIR_SIZE, MacroBaseDefaults.INPUT_RESERVOIR_SIZE),
             conf.getDecayType(),
             conf.getDouble(MacroBaseConf.MODEL_UPDATE_PERIOD, MacroBaseDefaults.MODEL_UPDATE_PERIOD),
             conf.getDouble(MacroBaseConf.DECAY_RATE, MacroBaseDefaults.DECAY_RATE),
             conf.getDecayType(),
             conf.getDouble(MacroBaseConf.MODEL_UPDATE_PERIOD, MacroBaseDefaults.MODEL_UPDATE_PERIOD));
    }

    public EWFeatureTransform(MacroBaseConf conf,
                              Iterator<Datum> input,
                              MacroBaseConf.TransformType transformType,
                              int warmupCount,
                              int sampleSize,
                              MacroBaseConf.PeriodType decayPeriodType,
                              double decayPeriod,
                              double decayRate,
                              MacroBaseConf.PeriodType trainingPeriodType,
                              double trainingPeriod) throws ConfigurationException {
        this.input = input;

        scorer = conf.constructTransform(transformType);

        reservoir = new ExponentiallyBiasedAChao<>(sampleSize, decayRate, conf.getRandom());

        decayer = new Periodic(decayPeriodType,
                               decayPeriod,
                               reservoir::advancePeriod);

        retrainer = new Periodic(trainingPeriodType,
                                 trainingPeriod,
                                 () -> scorer.train(reservoir.getReservoir()));

        List<Datum> warmupInput = new ArrayList<>(warmupCount);
        for(int i = 0; i < warmupCount; ++i) {
            Datum d = input.next();
            warmupInput.add(d);
            reservoir.insert(d);
            retrainer.runIfNecessary();
            decayer.runIfNecessary();
        }

        scorer.train(reservoir.getReservoir());
        for(Datum d : warmupInput) {
            warmupOutput.add(new Datum(d, scorer.score(d)));
        }
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public Datum next() {
        if(!warmupOutput.isEmpty()) {
            return warmupOutput.remove(0);
        }

        retrainer.runIfNecessary();
        decayer.runIfNecessary();

        Datum d = input.next();
        reservoir.insert(d);

        return new Datum(d, scorer.score(d));
    }
}
