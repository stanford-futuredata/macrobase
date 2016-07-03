package macrobase.bench;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MetricScalabilityTest extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(MetricScalabilityTest.class);

    @Override
    public MetricScalabilityTest initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        final int numPoints = 10000000;
        final double percentageOutliers = 0.01;

        for(Integer metricDimensions : Lists.newArrayList(128, 64, 32, 16, 8, 4, 2, 1)) {
            RandomGenerator g = new MersenneTwister(0);

            NormalDistribution inliers = new NormalDistribution(g, 10, 10);
            NormalDistribution outliers = new NormalDistribution(g, 70, 10);
            UniformRealDistribution extraneous = new UniformRealDistribution(g, 0, 80);

            List<Datum> data = new ArrayList<>();

            List<Double> thrus = new ArrayList<>();

            for(int it = 0; it < 5; ++it) {
                Random r = new Random();
                for (Integer i = 0; i < numPoints; ++i) {
                    Integer attr = null;

                    boolean isOutlier = ((((float) i / numPoints) < percentageOutliers));


                    if (attr == null) {
                        if (isOutlier) {
                            attr = conf.getEncoder().getIntegerEncoding(0, String.valueOf(1));
                        } else {
                            attr = conf.getEncoder().getIntegerEncoding(0, String.valueOf(2));
                        }
                    }

                    double[] pts = new double[metricDimensions];
                    for (int pno = 0; pno < metricDimensions; ++pno) {
                        double val;

                        if (isOutlier) {
                            val = outliers.sample();
                        } else {
                            val = inliers.sample();
                        }
                        pts[pno] = val;
                    }

                    data.add(new Datum(Lists.newArrayList(attr), new ArrayRealVector(pts)));
                }

                FeatureTransform normalizer = new LinearMetricNormalizer();
                normalizer.consume(data);
                data = normalizer.getStream().drain();

                System.gc();
                BatchScoreFeatureTransform bts = new BatchScoreFeatureTransform(conf, MacroBaseConf.TransformType.MCD);
                Stopwatch sw = Stopwatch.createStarted();
                bts.consume(data);
                bts.getStream().drain();
                sw.stop();

                double thru = ((double) data.size()) / sw.elapsed(TimeUnit.MICROSECONDS) * 1000000;

                log.error("MCD TIME: {} {} {}", metricDimensions, sw.elapsed(TimeUnit.MILLISECONDS),
                          thru);

                thrus.add(thru);
            }

            log.error("MCD AVGTHRU: {} {}", metricDimensions, thrus.stream().reduce((a, b) -> a+b).get()/thrus.size());
        }

        return Arrays.asList(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>()));
    }
}