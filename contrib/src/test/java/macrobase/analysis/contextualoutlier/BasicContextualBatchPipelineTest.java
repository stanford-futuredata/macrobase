package macrobase.analysis.contextualoutlier;

import com.google.common.collect.Lists;
import jdk.nashorn.internal.ir.annotations.Ignore;
import junit.framework.TestCase;
import macrobase.analysis.contextualoutlier.conf.ContextualConf;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.ContextualAnalysisResult;
import macrobase.conf.MacroBaseConf;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static junit.framework.TestCase.assertEquals;

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
                .set(MacroBaseConf.METRICS, Lists.newArrayList("A"))
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simpleContextual.csv")
                .set(ContextualConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, Lists.newArrayList())
                .set(ContextualConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, Lists.newArrayList("C1", "C2"))
                .set(ContextualConf.CONTEXTUAL_DENSECONTEXTTAU, 0.4)
                .set(ContextualConf.CONTEXTUAL_NUMINTERVALS, 10)
                .set(ContextualConf.CONTEXTUAL_OUTPUT_FILE, "target/temp.txt");
        conf.loadSystemProperties();

        BasicContextualBatchedPipeline pipeline = new BasicContextualBatchedPipeline();
        pipeline.initialize(conf);
        //only have one contextual outlier
        for(AnalysisResult curAR: pipeline.run()) {
            ContextualAnalysisResult ar = (ContextualAnalysisResult)curAR;
            Context context = ar.getContext();
            List<Interval> intervals = context.getIntervals();
            TestCase.assertEquals(intervals.size(), 1);
            assertEquals(intervals.get(0).getColumnName(), "C1");
            TestCase.assertEquals(intervals.get(0) instanceof IntervalDiscrete, true);
            assertEquals( ((IntervalDiscrete)intervals.get(0)).getValue(), 0);   
        }


    }
    
    public void testContextualAPI() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.99) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("C1", "C2", "C3")) // loader
                .set(MacroBaseConf.METRICS, Lists.newArrayList("A"))
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simpleContextual.csv")
                .set(ContextualConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, Lists.newArrayList())
                .set(ContextualConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, Lists.newArrayList("C1", "C2", "C3"))
                .set(ContextualConf.CONTEXTUAL_DENSECONTEXTTAU, 0.4)
                .set(ContextualConf.CONTEXTUAL_NUMINTERVALS, 10)
                .set(ContextualConf.CONTEXTUAL_API, "findContextsGivenOutlierPredicate")
                .set(ContextualConf.CONTEXTUAL_API_OUTLIER_PREDICATES, "C3 = c1")
                .set(ContextualConf.CONTEXTUAL_OUTPUT_FILE, "target/temp.txt");
        conf.loadSystemProperties();

        BasicContextualBatchedPipeline pipeline = new BasicContextualBatchedPipeline();
        pipeline.initialize(conf);
        
        for(AnalysisResult curAR: pipeline.run()) {
            ContextualAnalysisResult ar = (ContextualAnalysisResult)curAR;
            Context context = ar.getContext();
            List<Interval> intervals = context.getIntervals();
            TestCase.assertEquals(intervals.size(), 1);
            assertEquals(intervals.get(0).getColumnName(), "C1");
            TestCase.assertEquals(intervals.get(0) instanceof IntervalDiscrete, true);
            assertEquals( ((IntervalDiscrete)intervals.get(0)).getValue(), 0);
        }


    }
}
