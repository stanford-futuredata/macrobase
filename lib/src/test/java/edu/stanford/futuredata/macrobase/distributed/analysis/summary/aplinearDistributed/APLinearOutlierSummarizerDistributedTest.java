package edu.stanford.futuredata.macrobase.distributed.analysis.summary.aplinearDistributed;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class APLinearOutlierSummarizerDistributedTest {
    @Test
    public void testDistribution() throws Exception {
        DataFrame df = new DataFrame();
        String[] col1 = {"a1", "a2", "a1", "a1"};
        String[] col2 = {"b1", "b1", "b2", "b1"};
        String[] col3 = {"c1", "c1", "c1", "c2"};
        double[] counts = {100, 300, 400, 500};
        double[] oCounts = {30, 5, 5, 7};
        df.addStringColumn("col1", col1);
        df.addStringColumn("col2", col2);
        df.addStringColumn("col3", col3);
        df.addDoubleColumn("counts", counts);
        df.addDoubleColumn("oCounts", oCounts);

        List<String> explanationAttributes = Arrays.asList(
                "col1",
                "col2",
                "col3"
        );
        SparkConf conf = new SparkConf().setAppName("MacroBaseTest").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        APLOutlierSummarizerDistributed summ = new APLOutlierSummarizerDistributed(sparkContext);
        summ.setCountColumn("counts");
        summ.setOutlierColumn("oCounts");
        summ.setMinSupport(.1);
        summ.setMinRatioMetric(3.0);
        summ.setAttributes(explanationAttributes);
        summ.process(df);
        APLExplanation e = summ.getResults();
        assertEquals(1, e.getResults().size());
        assertTrue(e.prettyPrint().contains("col1=a1"));
        assertEquals(47.0, e.numOutliers(), 1e-10);

        sparkContext.stop();

    }
}
