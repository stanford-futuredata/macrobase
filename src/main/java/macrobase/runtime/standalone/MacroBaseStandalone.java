package macrobase.runtime.standalone;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import macrobase.MacroBase;
import macrobase.analysis.CoreAnalyzer;
import macrobase.analysis.result.AnalysisResult;
import macrobase.ingest.PostgresLoader;
import macrobase.ingest.SQLLoader;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseStandalone extends ConfiguredCommand<StandaloneConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseStandalone.class);

    public MacroBaseStandalone() {
        super("standalone", "Run task without starting server.");
    }

    @Override
    protected void run(Bootstrap<StandaloneConfiguration> bootstrap,
                       Namespace namespace,
                       StandaloneConfiguration configuration) throws Exception {
        SQLLoader loader = new PostgresLoader();
        loader.connect(configuration.getDbUrl());
        AnalysisResult result = CoreAnalyzer.analyze(loader,
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
