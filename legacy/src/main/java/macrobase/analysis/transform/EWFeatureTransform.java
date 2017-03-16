package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.sample.FlexibleDampedReservoir;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.Periodic;

import java.util.ArrayList;
import java.util.List;

public class EWFeatureTransform extends FeatureTransform {
    private final FlexibleDampedReservoir<Datum> reservoir;
    private final BatchTrainScore scorer;
    private final List<Datum> warmupInput = new ArrayList<>();
    private final int warmupCount;
    private int tupleCount = 0;

    private final MBStream<Datum> output = new MBStream<>();

    private final Periodic retrainer;
    private final Periodic decayer;

    public EWFeatureTransform(MacroBaseConf conf) throws ConfigurationException {
        this(conf,
             conf.getInt(MacroBaseConf.WARMUP_COUNT, MacroBaseDefaults.WARMUP_COUNT),
             conf.getInt(MacroBaseConf.INPUT_RESERVOIR_SIZE, MacroBaseDefaults.INPUT_RESERVOIR_SIZE),
             conf.getDecayType(),
             conf.getDouble(MacroBaseConf.MODEL_UPDATE_PERIOD, MacroBaseDefaults.MODEL_UPDATE_PERIOD),
             conf.getDouble(MacroBaseConf.DECAY_RATE, MacroBaseDefaults.DECAY_RATE),
             conf.getDecayType(),
             conf.getDouble(MacroBaseConf.MODEL_UPDATE_PERIOD, MacroBaseDefaults.MODEL_UPDATE_PERIOD));
    }

    public EWFeatureTransform(MacroBaseConf conf,
                              int warmupCount,
                              int sampleSize,
                              MacroBaseConf.PeriodType decayPeriodType,
                              double decayPeriod,
                              double decayRate,
                              MacroBaseConf.PeriodType trainingPeriodType,
                              double trainingPeriod) throws ConfigurationException {
        scorer = conf.constructTransform();

        reservoir = new FlexibleDampedReservoir<>(sampleSize, decayRate, conf.getRandom());

        decayer = new Periodic(decayPeriodType,
                               decayPeriod,
                               reservoir::advancePeriod);

        retrainer = new Periodic(trainingPeriodType,
                                 trainingPeriod,
                                 () -> scorer.train(reservoir.getReservoir()));

        this.warmupCount = warmupCount;
    }

    @Override
    public void initialize() {

    }

    @Override
    public void consume(List<Datum> records) {
        List<Datum> batchOutput = new ArrayList<>(records.size());
        for(Datum d : records) {
            tupleCount ++;

            if(tupleCount < warmupCount) {
                warmupInput.add(d);
                reservoir.insert(d);
                retrainer.runIfNecessary();
                decayer.runIfNecessary();
            } else {
                if(tupleCount == warmupCount) {
                    scorer.train(reservoir.getReservoir());
                    for(Datum di: warmupInput) {
                        batchOutput.add(new Datum(di, scorer.score(di)));
                    }

                    warmupInput.clear();
                }

                retrainer.runIfNecessary();
                decayer.runIfNecessary();
                reservoir.insert(d);
                batchOutput.add(new Datum(d, scorer.score(d)));
            }
        }

        output.add(batchOutput);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public MBStream<Datum> getStream() {
        return output;
    }
}
