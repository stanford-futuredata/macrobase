package macrobase.analysis.summary;

import com.google.common.base.Stopwatch;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.IterUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BatchSummarizer extends Summarizer {
    private FPGrowthEmerging fpg;
    protected final Double minSupport;
    protected final Double minOIRatio;

    public BatchSummarizer(MacroBaseConf conf, Iterator<OutlierClassificationResult> input) {
        super(conf, input);
        fpg = new FPGrowthEmerging();
        minOIRatio = conf.getDouble(MacroBaseConf.MIN_OI_RATIO, MacroBaseDefaults.MIN_OI_RATIO);
        minSupport = conf.getDouble(MacroBaseConf.MIN_SUPPORT, MacroBaseDefaults.MIN_SUPPORT);
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public Summary next() {
        List<Datum> outliers = new ArrayList<>();
        List<Datum> inliers = new ArrayList<>();
        for (OutlierClassificationResult result : IterUtils.iterable(input)) {
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

        return new Summary(isr,
                           inliers.size(),
                           outliers.size(),
                           sw.elapsed(TimeUnit.MILLISECONDS));
    }
}
