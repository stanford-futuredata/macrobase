package macrobase;

import java.util.List;
import java.util.concurrent.TimeUnit;

import macrobase.analysis.result.AnalysisResult;
import macrobase.runtime.MacroBaseServer;

import macrobase.runtime.command.MacroBasePipelineCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

/**
 * Hello world!
 */
public class MacroBase {
    public static final MetricRegistry metrics = new MetricRegistry();
    public static final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(MacroBase.class);

    public static void main(String[] args) throws Exception {
        log.info("Welcome to\n" +
                           "  _   _   _   _   _   _   _   _   _  \n" +
                           " / \\ / \\ / \\ / \\ / \\ / \\ / \\ / \\ / \\ \n" +
                           "( m | a | c | r | o | b | a | s | e )\n" +
                           " \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \n");

        MacroBaseServer.main(args);
    }

    public List<AnalysisResult> getPipelineCommandReturnedResults() {
        return MacroBasePipelineCommand.getReturnedResults();
    }
}
