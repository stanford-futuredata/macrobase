package macrobase.runtime.standalone.scoping;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import macrobase.MacroBase;
import macrobase.analysis.BatchAnalyzer;
import macrobase.analysis.ScopingAnalyzer;
import macrobase.analysis.result.AnalysisResult;
import macrobase.ingest.PostgresLoader;
import macrobase.ingest.SQLLoader;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseScopingCommand extends ConfiguredCommand<ScopingStandaloneConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseScopingCommand.class);

    public MacroBaseScopingCommand() {
        super("scoping", "Run scoping task without starting server.");
    }

    @Override
    protected void run(Bootstrap<ScopingStandaloneConfiguration> bootstrap,
                       Namespace namespace,
                       ScopingStandaloneConfiguration configuration) throws Exception {
        SQLLoader loader = new PostgresLoader();
        loader.connect(configuration.getDbUrl());

        ScopingAnalyzer analyzer = new ScopingAnalyzer();
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

        analyzer.analyze(loader,
                                                 configuration.getBaseQuery(),
                                                 //the following is for scoping
                                                 configuration.getCategoricalAttributes(),
                                                 configuration.getNumericalAttributes(),
                                                 configuration.getNumInterval(),
                                                 configuration.getMinFrequentSubSpaceRatio(),
                                                 configuration.getMaxSparseSubSpaceRatio());
        

        MacroBase.reporter.report();

        
    }
}
