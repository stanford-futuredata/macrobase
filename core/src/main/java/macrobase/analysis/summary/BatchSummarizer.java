package macrobase.analysis.summary;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BatchSummarizer extends Summarizer {
    private FPGrowthEmerging fpg;
    protected final Double minSupport;
    protected final Double minOIRatio;
    private MBStream<Summary> output = new MBStream<>();
    private final DatumEncoder encoder;

    public BatchSummarizer(MacroBaseConf conf) {
        fpg = new FPGrowthEmerging(conf.getBoolean(MacroBaseConf.ATTRIBUTE_COMBINATIONS,
                                                   MacroBaseDefaults.ATTRIBUTE_COMBINATIONS));
        minOIRatio = conf.getDouble(MacroBaseConf.MIN_OI_RATIO, MacroBaseDefaults.MIN_OI_RATIO);
        minSupport = conf.getDouble(MacroBaseConf.MIN_SUPPORT, MacroBaseDefaults.MIN_SUPPORT);
        encoder = conf.getEncoder();
    }

    @Override
    public MBStream<Summary> getStream() {
        return output;
    }

    @Override
    public void initialize() {

    }

    private Summary summary = null;

    @Override
    public void consume(List<OutlierClassificationResult> records) {
        List<Datum> outliers = new ArrayList<>();
        List<Datum> inliers = new ArrayList<>();
        for (OutlierClassificationResult result : records) {
            if (result.isOutlier()) {
                outliers.add(result.getDatum());
            } else {
                inliers.add(result.getDatum());
            }
        }
        Stopwatch sw = Stopwatch.createStarted();
        List<ItemsetResult> isr = fpg.getEmergingItemsetsWithMinSupport(
                inliers,
                outliers,
                minSupport,
                minOIRatio,
                encoder);

        summary = new Summary(isr,
                              inliers.size(),
                              outliers.size(),
                              sw.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public void shutdown() {

    }

    @Override
    public Summarizer summarize() {
        if (summary != null) {
            output.add(summary);
            summary = null;
        }

        return this;
    }
}
