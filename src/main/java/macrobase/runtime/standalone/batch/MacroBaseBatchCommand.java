package macrobase.runtime.standalone.batch;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import macrobase.MacroBase;
import macrobase.analysis.BatchAnalyzer;
import macrobase.analysis.result.AnalysisResult;
import macrobase.ingest.*;
import macrobase.runtime.standalone.BaseStandaloneConfiguration;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseBatchCommand extends ConfiguredCommand<BatchStandaloneConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseBatchCommand.class);

    public MacroBaseBatchCommand() {
        super("batch", "Run task without starting server.");
    }

    @Override
    protected void run(Bootstrap<BatchStandaloneConfiguration> bootstrap,
                       Namespace namespace,
                       BatchStandaloneConfiguration configuration) throws Exception {

        DataLoader loader;
        if (configuration.getDataLoaderType() == BaseStandaloneConfiguration.DataLoaderType.CSV_LOADER) {
            loader = new CsvLoader();
        } else if(configuration.useDiskCache()) {
            loader = new DiskCachingPostgresLoader(configuration.getDiskCacheDirectory());
        } else {
            loader = new PostgresLoader();
        }

        loader.setDatabaseCredentials(configuration.getDbUser(), configuration.getDbPassword());
        loader.connect(configuration.getDbUrl());

        BatchAnalyzer analyzer = new BatchAnalyzer(configuration);
        analyzer.setDetectorType(configuration.getDetectorType());
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

        analyzer.forceUsePercentile(configuration.usePercentile());
        analyzer.forceUseZScore(configuration.useZScore());
        analyzer.setAlphaMCD(configuration.getAlphaMCD());
        analyzer.setStoppingDeltaMCD(configuration.getStoppingDeltaMCD());

        AnalysisResult result = analyzer.analyze(loader,
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
