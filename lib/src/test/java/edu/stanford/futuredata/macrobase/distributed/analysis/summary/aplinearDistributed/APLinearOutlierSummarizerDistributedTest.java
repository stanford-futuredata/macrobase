package edu.stanford.futuredata.macrobase.distributed.analysis.summary.aplinearDistributed;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.distributed.datamodel.DistributedDataFrame;
import edu.stanford.futuredata.macrobase.distributed.ingest.CSVDataFrameParserDistributed;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class APLinearOutlierSummarizerDistributedTest {
    @Test
    public void testTransformDataFrame() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put("_OUTLIER", Schema.ColType.DOUBLE);
        colTypes.put("_COUNT", Schema.ColType.DOUBLE);

        SparkConf conf = new SparkConf().setAppName("MacroBaseTest").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        CSVDataFrameParserDistributed loader = new CSVDataFrameParserDistributed("src/test/resources/distributedTest.csv",
                Arrays.asList("col1", "col2", "col3", "_OUTLIER", "_COUNT"));
        loader.setColumnTypes(colTypes);

        DistributedDataFrame df = loader.load(sparkContext, 2);
        List<String> explanationAttributes = Arrays.asList(
                "col1",
                "col2",
                "col3"
        );

        JavaPairRDD<String[], double[]> testRDD =
                APLSummarizerDistributed.transformDataFrame(df, explanationAttributes,
                        "_OUTLIER", "_COUNT");

        List<Tuple2<String[], double[]>> collectedCSV = testRDD.collect();
        sparkContext.stop();

        assertEquals(4, collectedCSV.size());
        assertEquals(3, collectedCSV.get(0)._1.length);
        assertEquals(2, collectedCSV.get(0)._2.length);

        assertEquals(30.0, collectedCSV.get(0)._2[0]);
        assertEquals(100.0, collectedCSV.get(0)._2[1]);
        assertEquals("a1", collectedCSV.get(0)._1[0]);
        assertEquals("b1", collectedCSV.get(0)._1[1]);
        assertEquals("c1", collectedCSV.get(0)._1[2]);
        assertEquals(5.0, collectedCSV.get(1)._2[0]);
        assertEquals(300.0, collectedCSV.get(1)._2[1]);
        assertEquals("a2", collectedCSV.get(1)._1[0]);
        assertEquals("b1", collectedCSV.get(1)._1[1]);
        assertEquals("c1", collectedCSV.get(1)._1[2]);
        assertEquals(5.0, collectedCSV.get(2)._2[0]);
        assertEquals(400.0, collectedCSV.get(2)._2[1]);
        assertEquals("a1", collectedCSV.get(2)._1[0]);
        assertEquals("b2", collectedCSV.get(2)._1[1]);
        assertEquals("c1", collectedCSV.get(2)._1[2]);
        assertEquals(7.0, collectedCSV.get(3)._2[0]);
        assertEquals(500.0, collectedCSV.get(3)._2[1]);
        assertEquals("a1", collectedCSV.get(3)._1[0]);
        assertEquals("b1", collectedCSV.get(3)._1[1]);
        assertEquals("c2", collectedCSV.get(3)._1[2]);

    }

    @Test
    public void testDistribution() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put("_OUTLIER", Schema.ColType.DOUBLE);
        colTypes.put("_COUNT", Schema.ColType.DOUBLE);

        SparkConf conf = new SparkConf().setAppName("MacroBaseTest").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        CSVDataFrameParserDistributed loader = new CSVDataFrameParserDistributed("src/test/resources/distributedTest.csv",
                Arrays.asList("col1", "col2", "col3", "_OUTLIER", "_COUNT"));
        loader.setColumnTypes(colTypes);

        DistributedDataFrame df = loader.load(sparkContext, 1);

        List<String> explanationAttributes = Arrays.asList(
                "col1",
                "col2",
                "col3"
        );
        APLOutlierSummarizerDistributed summ = new APLOutlierSummarizerDistributed();
        summ.setCountColumn("_COUNT");
        summ.setOutlierColumn("_OUTLIER");
        summ.setMinSupport(.1);
        summ.setMinRatioMetric(3.0);
        summ.setAttributes(explanationAttributes);
        summ.process(df);
        APLExplanation e = summ.getResults();
        sparkContext.stop();
        assertEquals(1, e.getResults().size());
        assertTrue(e.prettyPrint().contains("col1=a1"));
        assertEquals(47.0, e.numOutliers(), 1e-10);

    }
}
