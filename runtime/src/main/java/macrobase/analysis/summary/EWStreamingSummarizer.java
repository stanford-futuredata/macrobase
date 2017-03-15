package macrobase.analysis.summary;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.DatumEncoder;
import macrobase.util.Periodic;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class EWStreamingSummarizer extends Summarizer {
    private final ExponentiallyDecayingEmergingItemsets streamingSummarizer;
    private final Periodic summaryUpdater;
    private final Periodic summarizationTimer;
    private int count;
    private final DatumEncoder encoder;

    private final MBStream<Summary> output = new MBStream<>();

    private boolean needsSummarization = false;

    public EWStreamingSummarizer(MacroBaseConf conf) throws ConfigurationException {
        this(conf, -1);
    }

    public EWStreamingSummarizer(MacroBaseConf conf,
                                 int maximumSummaryDelay) throws ConfigurationException {
        Double summaryPeriod = conf.getDouble(MacroBaseConf.SUMMARY_UPDATE_PERIOD,
                                              MacroBaseDefaults.SUMMARY_UPDATE_PERIOD);
        Double decayRate = conf.getDouble(MacroBaseConf.DECAY_RATE, MacroBaseDefaults.DECAY_RATE);
        Integer outlierItemSummarySize = conf.getInt(MacroBaseConf.OUTLIER_ITEM_SUMMARY_SIZE,
                                                     MacroBaseDefaults.OUTLIER_ITEM_SUMMARY_SIZE);
        Integer inlierItemSummarySize = conf.getInt(MacroBaseConf.INLIER_ITEM_SUMMARY_SIZE,
                                                    MacroBaseDefaults.INLIER_ITEM_SUMMARY_SIZE);

        Double minOIRatio = conf.getDouble(MacroBaseConf.MIN_OI_RATIO, MacroBaseDefaults.MIN_OI_RATIO);
        Double minSupport = conf.getDouble(MacroBaseConf.MIN_SUPPORT, MacroBaseDefaults.MIN_SUPPORT);

        List<String> attributes = conf.getStringList(MacroBaseConf.ATTRIBUTES);

        encoder = conf.getEncoder();

        streamingSummarizer = new ExponentiallyDecayingEmergingItemsets(inlierItemSummarySize,
                                                                        outlierItemSummarySize,
                                                                        minSupport,
                                                                        minOIRatio,
                                                                        decayRate,
                                                                        attributes.size(),
                                                                        conf.getBoolean(MacroBaseConf.ATTRIBUTE_COMBINATIONS,
                                                                                        MacroBaseDefaults.ATTRIBUTE_COMBINATIONS));

        summaryUpdater = new Periodic(conf.getDecayType(),
                                      summaryPeriod,
                                      streamingSummarizer::markPeriod);

        summarizationTimer = new Periodic(conf.getDecayType(),
                                          maximumSummaryDelay,
                                          () -> needsSummarization = true);
    }

    @Override
    public MBStream<Summary> getStream() {
        return output;
    }

    @Override
    public void initialize() {

    }

    @Override
    public void consume(List<OutlierClassificationResult> records) {
        for(OutlierClassificationResult result : records) {
            count++;
            summaryUpdater.runIfNecessary();
            summarizationTimer.runIfNecessary();

            if(result.isOutlier()) {
                streamingSummarizer.markOutlier(result.getDatum());
            } else {
                streamingSummarizer.markInlier(result.getDatum());
            }
        }
    }

    @Override
    public void shutdown() {

    }

    @Override
    public Summarizer summarize() {
        Stopwatch sw = Stopwatch.createStarted();
        List<ItemsetResult> isr = streamingSummarizer.getItemsets(encoder);
        output.add(new Summary(isr,
                               streamingSummarizer.getInlierCount(),
                               streamingSummarizer.getOutlierCount(),
                               sw.elapsed(TimeUnit.MILLISECONDS)));
        return this;
    }
}
