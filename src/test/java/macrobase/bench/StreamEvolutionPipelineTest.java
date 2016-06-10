package macrobase.bench;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.analysis.stats.MAD;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.summary.count.AmortizedMaintenanceCounter;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StreamEvolutionPipelineTest extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(StreamEvolutionPipelineTest.class);

    @Override
    public StreamEvolutionPipelineTest initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        runOnConf("MB", .99999999, 100, false);
        runOnConf("EVERY", .999999 , 100, true);
        runOnConf("UNIFORM", 0, 0, false);


        return Arrays.asList(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>()));
    }

    private void runOnConf(String prefix, double decayrate, int decaysPerInterval, boolean every) throws Exception {
        Random r = new Random(0);
        int NUM_SPIKE = 10000;
        int SPIKE_LEN = 4;

        int INTERVAL_LEN = 1000000;

        int SPIKE_REPEAT_FACTOR = 10;

        int capacity = 200;
        int numServers = 100;
        int badServerOne = 1;

        int ptlograte = 1000;

        int retrainRate = 100;
        int reportsPerInterval = 50;

        int reservoirWarmup = 1000;
        int percentileWarmup = 2000;


        List<Integer> pointsPerInterval = Lists.newArrayList(INTERVAL_LEN,
                                                             INTERVAL_LEN, INTERVAL_LEN,
                                                             INTERVAL_LEN, INTERVAL_LEN,
                                                             INTERVAL_LEN, INTERVAL_LEN, INTERVAL_LEN);



        RandomGenerator g = new MersenneTwister(0);

        NormalDistribution inliers = new NormalDistribution(g, 10, 10);
        NormalDistribution outliers = new NormalDistribution(g, 70, 10);
        UniformRealDistribution noise = new UniformRealDistribution(g, 70, 200);

        MAD m = new MAD(conf);
        ExponentiallyBiasedAChao<Datum> reservoir = new ExponentiallyBiasedAChao<>(capacity, decayrate);
        ExponentiallyBiasedAChao<Double> scoreReservoir = new ExponentiallyBiasedAChao<>(capacity, decayrate);

        AmortizedMaintenanceCounter inlierCount = new AmortizedMaintenanceCounter(100000);
        AmortizedMaintenanceCounter outlierCount = new AmortizedMaintenanceCounter(100000);

        double threshPctile = 0.95;
        double curThresh = 0;


        int reportNo = 0;



        for(int intervalNo = 0; intervalNo < pointsPerInterval.size(); ++intervalNo) {

            List<Double> valsSinceReport = new ArrayList<>();

            reservoir.advancePeriod();
            scoreReservoir.advancePeriod();


            // interval 3, change default outlier and inlier ratios
            if(intervalNo == 3) {
                inliers = new NormalDistribution(g, 40, 10);
                outliers = new NormalDistribution(g, -10, 10);
            }

            if(intervalNo == 6) {
                outliers = new NormalDistribution(85, 10);
            }




            log.info("INTERVAL: {}", intervalNo);
            int intervalPoints = pointsPerInterval.get(intervalNo);

            Set<Integer> reportTimes = new HashSet<>();

            int reportspacing = INTERVAL_LEN / reportsPerInterval;
            for(int i = 1; i < reportsPerInterval; ++i) {
                reportTimes.add(reportspacing*i);
            }

            log.info("{}", reportTimes);


            for(int ptno = 0; ptno < intervalPoints; ++ptno) {

                boolean spiking = intervalNo == 6 && ptno > reportspacing*24 + 2000 && ptno < NUM_SPIKE*SPIKE_LEN + reportspacing * 24 + 4000;

                int server = r.nextInt(numServers);
                double val;

                // if we're in 1 or 4, then start the misbehaving server
                if(server == badServerOne && (intervalNo == 1 || intervalNo == 4) && ptno > intervalPoints/4) {
                    val = outliers.sample();
                } else {
                    val = inliers.sample();
                }

                // special interval, first we have a crazy spike, then the other server behaves poorly
                if(spiking) {
                    if (server != badServerOne) {
                        val = noise.sample();
                    } else {
                        val = inliers.sample();
                    }
                }

                // log points
                if(server == badServerOne) {
                    log.debug("BADSERVER_VAL {} {} {}", prefix, reportNo+(valsSinceReport.size()/((float)intervalPoints/reportsPerInterval)), val);
                }
                else if (ptno % ptlograte == 0) {
                    log.debug("OTHER_VAL {} {} {}", prefix, reportNo+(valsSinceReport.size()/((float)intervalPoints/reportsPerInterval)), val);
                }

                Datum d = new Datum(Lists.newArrayList(server), val);


                if(!spiking) {
                    reservoir.insert(d);
                    valsSinceReport.add(val);
                } else {
                    for(int i = 0; i < SPIKE_REPEAT_FACTOR; ++i) {
                        reservoir.insert(d);
                        valsSinceReport.add(val);
                    }
                }

                // warmup
                if(intervalNo == 0 && ptno < reservoirWarmup) {
                    continue;
                }

                // retrain model (not critical)
                if((intervalNo == 0 && ptno == reservoirWarmup)
                   || (intervalNo == 0 && ptno > reservoirWarmup && ptno % retrainRate == 0)
                   || (intervalNo != 0 && ptno % retrainRate == 0)) {
                    m.train(reservoir.getReservoir());
                }

                // actually report this
                if(reportTimes.contains(ptno)) {
                    int numPoints = valsSinceReport.size();
                   Double avg = valsSinceReport.stream().reduce((a, b) -> a + b).get()/valsSinceReport.size();

                    List<Datum> resPoints = reservoir.getReservoir();
                    Collections.sort(resPoints, (a, b) -> Double.compare(a.norm(), b.norm()));
                    double res_avg = resPoints.stream().map(i -> i.norm()).reduce((a, b)-> a+b).get()/resPoints.size(); //resPoints.get((int)(resPoints.size()*.95)).norm();
                    //
                    log.info("OI: {} {}", outlierCount.getCount(badServerOne), inlierCount.getCount(badServerOne));

                    double oi = outlierCount.getCount(badServerOne)/inlierCount.getCount(badServerOne);
                    log.debug("OUTPUT: {} {} {} {} {} {}", prefix, reportNo, numPoints, avg, res_avg, oi);

                    valsSinceReport.clear();

                    reportNo+= 1;
                }

                // if we're advancing every single tuple, then do so.
                if(every) {
                    reservoir.advancePeriod();
                    scoreReservoir.advancePeriod();
                }


                if(decaysPerInterval != 0 && ptno % ((int)((double)intervalPoints)/decaysPerInterval) == 0) {
                    inlierCount.multiplyAllCounts(.5);
                    outlierCount.multiplyAllCounts(.5);


                    if(!every) {
                        reservoir.advancePeriod();
                        scoreReservoir.advancePeriod();
                    }
                }

                // recompute thresholds based on reservoir
                if((intervalNo == 0 && ptno == percentileWarmup)
                   || (intervalNo == 0 && ptno > percentileWarmup && ptno % retrainRate == 0)
                   || (intervalNo != 0 && ptno % retrainRate == 0)) {
                    List<Double> scores = scoreReservoir.getReservoir();
                    Collections.sort(scores);
                    curThresh = scores.get((int)(threshPctile * scores.size()));
                }

                double score = m.score(d);

                if(!spiking) {
                    scoreReservoir.insert(score);
                } else {
                    for(int i = 0; i < SPIKE_REPEAT_FACTOR; ++i) {
                        scoreReservoir.insert(score);
                    }
                }


                if(intervalNo == 0 && ptno < percentileWarmup) {
                    continue;
                }

                if(score > curThresh) {
                    outlierCount.observe(server, 1);
                } else {
                    inlierCount.observe(server, 1);
                }
            }
        }

    }
}