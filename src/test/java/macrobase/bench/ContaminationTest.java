package macrobase.bench;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.stats.MAD;
import macrobase.analysis.stats.MinCovDet;
import macrobase.analysis.stats.ZScore;
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

public class ContaminationTest extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(ContaminationTest.class);

    @Override
    public ContaminationTest initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    private double get_fscore(BatchTrainScore bts, double thresh, List<Datum> data, HashSet<Datum> outlierPoints) {
        bts.train(data);


        double btsOutlierCount = 0;
        double correctOutliers = 0;
        for(Datum d : data) {
            if(bts.getZScoreEquivalent(bts.score(d)) >= thresh) {
                btsOutlierCount++;

                if(outlierPoints.contains(d)) {
                    correctOutliers++;
                }
            }
        }

        double precision = correctOutliers / btsOutlierCount;
        double recall = correctOutliers / outlierPoints.size();

        return 2* (precision*recall)/(precision + recall);
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        final int numPoints = 10000000;

        Integer metricDimensions = 2;
        for(Double percentageOutliers : Lists.newArrayList(0.01, .05, .1, .15, .2, .25, .3, .35, .4, .45, .5)) {
            RandomGenerator g = new MersenneTwister(0);

            NormalDistribution inliers = new NormalDistribution(g, 10, 10);
            NormalDistribution outliers = new NormalDistribution(g, 70, 10);
            UniformRealDistribution extraneous = new UniformRealDistribution(g, 0, 80);

            List<Datum> data = new ArrayList<>();

            HashSet<Datum> outlierPoints = new HashSet<>();

            List<Double> mad_results = new ArrayList<>();
            List<Double> mcd_results = new ArrayList<>();
            List<Double> zscore_results = new ArrayList<>();


            for (int it = 0; it < 5; ++it) {
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
                    double val;

                    if (isOutlier) {
                        val = outliers.sample();
                    } else {
                        val = inliers.sample();
                    }

                    for (int pno = 0; pno < metricDimensions; ++pno) {
                        pts[pno] = val;
                    }

                    Datum d = new Datum(Lists.newArrayList(attr), new ArrayRealVector(pts));
                    data.add(d);
                    if (isOutlier) {
                        outlierPoints.add(d);
                    }
                }

                MAD mad = new MAD(conf);
                mad.train(data);
                double fs = get_fscore(mad, 3.0, data, outlierPoints);
                mad_results.add(fs);
                log.info("MAD {} {}", percentageOutliers, fs);

                MinCovDet mcd = new MinCovDet(conf);
                mad.train(data);
                fs = get_fscore(mcd, 3.0, data, outlierPoints);
                mcd_results.add(fs);
                log.info("MCD {} {}", percentageOutliers, fs);

                ZScore zscore = new ZScore(conf);
                zscore.train(data);
                fs = get_fscore(zscore, 3.0, data, outlierPoints);
                zscore_results.add(fs);
                log.info("ZSCORE {} {}", percentageOutliers, fs);
            }

            log.info("{} MAD_AVG {} MCD_AVG {} ZSCORE_AVG {}", percentageOutliers,
                     mad_results.stream().reduce((a, b)-> a+b).get()/mad_results.size(),
                     mcd_results.stream().reduce((a, b)-> a+b).get()/mcd_results.size(),
                     zscore_results.stream().reduce((a, b)-> a+b).get()/zscore_results.size());
        }

        return Arrays.asList(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>()));
    }
}