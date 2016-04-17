package macrobase.pipeline;

import com.google.common.collect.Lists;

import macrobase.analysis.contextualoutlier.Context;
import macrobase.analysis.contextualoutlier.Interval;
import macrobase.analysis.contextualoutlier.IntervalDiscrete;
import macrobase.analysis.pipeline.BasicContextualBatchedPipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.ContextualAnalysisResult;
import macrobase.conf.MacroBaseConf;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.TestCase.assertEquals;

import java.util.List;

public class BasicContextualBatchPipelineTest {
    private static final Logger log = LoggerFactory.getLogger(BasicContextualBatchPipelineTest.class);
    
    public void testContextualMADAnalyzer() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.99) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList()) // loader
                .set(MacroBaseConf.LOW_METRICS, Lists.newArrayList())
                .set(MacroBaseConf.HIGH_METRICS, Lists.newArrayList("A"))
                .set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "")
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simpleContextual.csv")
                .set(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, Lists.newArrayList())
                .set(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, Lists.newArrayList("C1", "C2"))
                .set(MacroBaseConf.CONTEXTUAL_DENSECONTEXTTAU, 0.4)
                .set(MacroBaseConf.CONTEXTUAL_NUMINTERVALS, 10)
                .set(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE, "temp.txt");
        conf.loadSystemProperties();
        conf.sanityCheckBatch();

        BasicContextualBatchedPipeline pipeline = new BasicContextualBatchedPipeline();
        pipeline.initialize(conf);
        //only have one contextual outlier
        for(AnalysisResult curAR: pipeline.run()) {
            ContextualAnalysisResult ar = (ContextualAnalysisResult)curAR;
            Context context = ar.getContext();
            List<Interval> intervals = context.getIntervals();
            assertEquals(intervals.size(), 1);
            assertEquals(intervals.get(0).getColumnName(), "C1");
            assertEquals(intervals.get(0) instanceof IntervalDiscrete, true);
            assertEquals( ((IntervalDiscrete)intervals.get(0)).getValue(), 0);   
        }


    }

    @Test
    public void testContextualAPI() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.99) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList()) // loader
                .set(MacroBaseConf.LOW_METRICS, Lists.newArrayList())
                .set(MacroBaseConf.HIGH_METRICS, Lists.newArrayList("A"))
                .set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "")
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simpleContextual.csv")
                .set(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, Lists.newArrayList())
                .set(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, Lists.newArrayList("C1", "C2", "C3"))
                .set(MacroBaseConf.CONTEXTUAL_DENSECONTEXTTAU, 0.4)
                .set(MacroBaseConf.CONTEXTUAL_NUMINTERVALS, 10)
                .set(MacroBaseConf.CONTEXTUAL_API, "findContextsGivenOutlierPredicate")
                .set(MacroBaseConf.CONTEXTUAL_API_OUTLIER_PREDICATES, "C3 = c1")
                .set(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE, "temp.txt");
        conf.loadSystemProperties();
        conf.sanityCheckBatch();

        BasicContextualBatchedPipeline pipeline = new BasicContextualBatchedPipeline();
        pipeline.initialize(conf);
        
        for(AnalysisResult curAR: pipeline.run()) {
            ContextualAnalysisResult ar = (ContextualAnalysisResult)curAR;
            Context context = ar.getContext();
            List<Interval> intervals = context.getIntervals();
            assertEquals(intervals.size(), 1);
            assertEquals(intervals.get(0).getColumnName(), "C1");
            assertEquals(intervals.get(0) instanceof IntervalDiscrete, true);
            assertEquals( ((IntervalDiscrete)intervals.get(0)).getValue(), 0);
        }


    }
}
