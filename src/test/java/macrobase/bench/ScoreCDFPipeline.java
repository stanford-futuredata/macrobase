package macrobase.bench;

import com.google.common.base.Stopwatch;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ScoreCDFPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(ScoreCDFPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    private double median(List<Double> a ){
        a.sort((a1, b1) -> a1.compareTo(b1));
        double median;
        if (a.size() % 2 == 0)
            median = (a.get(a.size()/2) + a.get(a.size()/2-1))/2;
        else
            median = a.get(a.size()/2);

        return median;
    }

    private double average(List<Double> a) {
        return a.stream().mapToDouble(i -> i).average().getAsDouble();
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        DataIngester ingester = conf.constructIngester();
        List<Datum> data = ingester.getStream().drain();
        System.gc();

        FeatureTransform ft = new BatchScoreFeatureTransform(conf, conf.getTransformType());
        ft.consume(data);

        List<Datum> scored = ft.getStream().drain();

        scored.sort((a, b) -> ((Double) a.getMetrics().getNorm())
                .compareTo(b.getMetrics().getNorm()));


        for(double i = 0; i < 1; i += 0.001) {
            System.out.printf("P %f %f\n", i, scored.get((int)(scored.size()*i)).getMetrics().getNorm());
        }
        System.out.printf("P 1 %f\n", scored.get(scored.size()-1).getMetrics().getNorm());

        Map<Integer, ArrayList<Double>> itemAverages = new HashMap<>();

        for(Datum d : scored) {
            for(Integer item : d.getAttributes()) {
                if(!itemAverages.containsKey(item)) {
                    itemAverages.put(item, new ArrayList<>());
                }

                ArrayList a = itemAverages.get(item);
                a.add(d.getMetrics().getNorm());
            }
        }

        List<Double> averages = new ArrayList<>();
        List<Double> medians = new ArrayList<>();

        for(List<Double> l : itemAverages.values()) {
            averages.add(average(l));
            medians.add(median(l));
        }

        averages.sort((a, b)->a.compareTo(b));
        medians.sort((a, b)->a.compareTo(b));


        for(double i = 0; i < 1; i += 0.001) {
            System.out.printf("IA %f %f\n", i, averages.get((int) (averages.size() * i)));
        }
        System.out.printf("IA 1 %f\n", averages.get(averages.size() - 1));


        for(double i = 0; i < 1; i += 0.001) {
            System.out.printf("IM %f %f\n", i, medians.get((int) (medians.size() * i)));
        }
        System.out.printf("IM 1 %f\n", medians.get(medians.size() - 1));

        return Arrays.asList(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>()));
    }
}