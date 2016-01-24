package macrobase.runtime.standalone.streaming;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import macrobase.MacroBase;
import macrobase.analysis.StreamingAnalyzer;
import macrobase.analysis.result.AnalysisResult;
import macrobase.ingest.DiskCachingPostgresLoader;
import macrobase.ingest.PostgresLoader;
import macrobase.ingest.SQLLoader;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseStreamingCommand extends ConfiguredCommand<StreamingStandaloneConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseStreamingCommand.class);

    public MacroBaseStreamingCommand() {
        super("streaming", "Run streaming task without starting server.");
    }

    @Override
    protected void run(Bootstrap<StreamingStandaloneConfiguration> bootstrap,
                       Namespace namespace,
                       StreamingStandaloneConfiguration configuration) throws Exception {
        SQLLoader loader;

        if(configuration.useDiskCache()) {
            loader = new DiskCachingPostgresLoader(configuration.getDiskCacheDirectory());
        } else {
            loader = new PostgresLoader();
        }

        loader.connect(configuration.getDbUrl());

        StreamingAnalyzer analyzer = new StreamingAnalyzer();
        analyzer.setMinInlierRatio(configuration.getMinInlierRatio());
        analyzer.setMinSupport(configuration.getMinSupport());
        analyzer.setTargetPercentile(configuration.getTargetPercentile());
        analyzer.setZScore(configuration.getzScore());

        // todo: either use a single boolean or check this
        // at parse time
        if(configuration.usePercentile() && configuration.useZScore()) {
            log.error("Can only select one of usePercentile or useZScore; exiting!");
            return;
        }
        else if(!configuration.usePercentile() && !configuration.useZScore()) {
            log.error("Must select at least one of usePercentile or useZScore; exiting!");
            return;
        }

        if(configuration.useRealTimePeriod() && configuration.useRealTimePeriod()) {
            log.error("Can only select one of useRealTimePeriod or useTupleCountPeriod; exiting!");
            return;
        }
        else if(!configuration.useRealTimePeriod() && !configuration.useTupleCountPeriod()) {
            log.error("Must select at least one of useRealTimePeriod or useTupleCountPeriod; exiting!");
            return;
        }

        analyzer.forceUsePercentile(configuration.usePercentile());
        analyzer.forceUseZScore(configuration.useZScore());

        analyzer.setDetectorType(configuration.getDetectorType());
        analyzer.setDecayRate(configuration.getDecayRate());
        analyzer.setInputReservoirSize(configuration.getInputReservoirSize());
        analyzer.setScoreReservoirSize(configuration.getScoreReservoirSize());
        analyzer.setSummaryPeriod(configuration.getSummaryRefreshPeriod());
        analyzer.useRealTimeDecay(configuration.useRealTimePeriod());
        analyzer.useTupleCountDecay(configuration.useTupleCountPeriod());
        analyzer.setModelRefreshPeriod(configuration.getModelRefreshPeriod());
        analyzer.setWarmupCount(configuration.getWarmupCount());
        analyzer.setInlierItemSummarySize(configuration.getInlierItemSummarySize());
        analyzer.setOutlierItemSummarySize(configuration.getOutlierItemSummarySize());
        analyzer.setMinSupportOutlier(configuration.getMinSupport());
        analyzer.setMinRatio(configuration.getMinInlierRatio());
        analyzer.setTracing(configuration.traceRuntime());
        
        analyzer.setAlphaMCD(configuration.getAlphaMCD());
        analyzer.setStoppingDeltaMCD(configuration.getStoppingDeltaMCD());

        AnalysisResult result = analyzer.analyzeOnePass(loader,
                                                             configuration.getTargetAttributes(),
                                                             configuration.getTargetLowMetrics(),
                                                             configuration.getTargetHighMetrics(),
                                                             configuration.getBaseQuery());
        if(result.getItemSets().size() > 1000) {
            log.warn("Very large result set! {}; truncating to 1000", result.getItemSets().size());
            result.setItemSets(result.getItemSets().subList(0, 1000));
        }

        MacroBase.reporter.report();

        log.info("Result: {}", result.prettyPrint());
    }
}
