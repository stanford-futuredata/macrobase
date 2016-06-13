package macrobase.bench;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by pbailis on 6/13/16.
 */
public class CppDumpPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(SummaryComparePipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        Stopwatch sw1 = Stopwatch.createStarted();
        DataIngester ingester = conf.constructIngester();

        List<Datum> data = ingester.getStream().drain();
        System.gc();

        final String inputFile = conf.getString(MacroBaseConf.QUERY_NAME)+".csv";


        FileWriter f = new FileWriter(inputFile);
        BufferedWriter bw = new BufferedWriter(f);

        for(Datum d: data) {
            bw.append(String.valueOf(d.getMetrics().getEntry(0)));
            bw.append(',');
            bw.append(String.valueOf(d.getAttributes().get(0)));
            bw.append('\n');
        }

        bw.close();

        return Arrays.asList(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>()));
    }
}
