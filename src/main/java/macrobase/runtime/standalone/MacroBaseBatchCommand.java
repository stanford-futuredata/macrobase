package macrobase.runtime.standalone;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import macrobase.MacroBase;
import macrobase.analysis.pipeline.BasicBatchedPipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseBatchCommand extends ConfiguredCommand<MacroBaseConf> {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseBatchCommand.class);

    public MacroBaseBatchCommand() {
        super("batch", "Run task without starting server.");
    }

    @Override
    protected void run(Bootstrap<MacroBaseConf> bootstrap,
                       Namespace namespace,
                       MacroBaseConf configuration) throws Exception {
        configuration.loadSystemProperties();
        BasicBatchedPipeline analyzer = new BasicBatchedPipeline(configuration);

        AnalysisResult result = analyzer.next();
        if (result.getItemSets().size() > 1000) {
            log.warn("Very large result set! {}; truncating to 1000", result.getItemSets().size());
            result.setItemSets(result.getItemSets().subList(0, 1000));
        }

        MacroBase.reporter.report();

        log.info("Result: {}", result);
    }
}
