package macrobase;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.outlier.ZScore;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.analysis.summary.count.ExactCount;
import macrobase.analysis.summary.itemset.FPGrowth;
import macrobase.analysis.summary.itemset.Apriori;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.PostgresLoader;
import macrobase.ingest.SQLLoader;
import macrobase.runtime.MacroBaseServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class MacroBase
{
    public static final MetricRegistry metrics = new MetricRegistry();
    public static final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                                                    .convertRatesTo(TimeUnit.SECONDS)
                                                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                    .build();

    private static final Logger log = LoggerFactory.getLogger(MacroBase.class);

    public static void main( String[] args ) throws Exception
    {
        System.out.println("Welcome to\n" +
                           "  _   _   _   _   _   _   _   _   _  \n" +
                           " / \\ / \\ / \\ / \\ / \\ / \\ / \\ / \\ / \\ \n" +
                           "( m | a | c | r | o | b | a | s | e )\n" +
                           " \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \\_/ \n");

        //benchmark();

        MacroBaseServer.main(args);
    }
}
