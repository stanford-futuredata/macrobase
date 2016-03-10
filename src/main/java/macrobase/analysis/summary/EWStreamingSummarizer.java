package macrobase.analysis.summary;

import com.google.common.base.Stopwatch;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.util.Periodic;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EWStreamingSummarizer extends Summarizer {
    private final ExponentiallyDecayingEmergingItemsets streamingSummarizer;
    private final Periodic summaryUpdater;
    private final Periodic summarizationTimer;
    private int count;

    private boolean needsSummarization = false;

    public EWStreamingSummarizer(MacroBaseConf conf,
                                 Iterator<OutlierClassificationResult> input) throws ConfigurationException {
        this(conf, input, -1);
    }

    public EWStreamingSummarizer(MacroBaseConf conf,
                                 Iterator<OutlierClassificationResult> classificationResults,
                                 int maximumSummaryDelay) throws ConfigurationException {
        super(conf, classificationResults);

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

        streamingSummarizer = new ExponentiallyDecayingEmergingItemsets(inlierItemSummarySize,
                                                                        outlierItemSummarySize,
                                                                        minSupport,
                                                                        minOIRatio,
                                                                        decayRate,
                                                                        attributes.size());

        summaryUpdater = new Periodic(conf.getDecayType(),
                                      summaryPeriod,
                                      streamingSummarizer::markPeriod);

        summarizationTimer = new Periodic(conf.getDecayType(),
                                          maximumSummaryDelay,
                                          () -> needsSummarization = true);
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public Summary next() {
        while(input.hasNext() && !needsSummarization) {
            count++;
            summaryUpdater.runIfNecessary();
            summarizationTimer.runIfNecessary();

            OutlierClassificationResult result = input.next();

            if(result.isOutlier()) {
                streamingSummarizer.markOutlier(result.getDatum());
            } else {
                streamingSummarizer.markInlier(result.getDatum());
            }
        }

        Stopwatch sw = Stopwatch.createStarted();
        List<ItemsetResult> isr = streamingSummarizer.getItemsets(encoder);
        return new Summary(isr,
                           streamingSummarizer.getInlierCount(),
                           streamingSummarizer.getOutlierCount(),
                           sw.elapsed(TimeUnit.MILLISECONDS));
    }
}
