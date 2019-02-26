package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class PredicateCubeClassifierTest {
    private DataFrame df;

    @Before
    public void setUp() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        List<String> requiredColumns = new ArrayList<>(Arrays.asList(
                "location","version","count","mean","std"
        ));
        colTypes.put("count", Schema.ColType.DOUBLE);
        colTypes.put("mean", Schema.ColType.DOUBLE);
        colTypes.put("std", Schema.ColType.DOUBLE);
        CSVDataFrameParser loader = new CSVDataFrameParser(
                "src/test/resources/sample_cubed.csv",
                requiredColumns
        );
        loader.setColumnTypes(colTypes);
        df = loader.load();
    }

    @Test
    public void testSimple() throws Exception {
        PredicateCubeClassifier pc = new PredicateCubeClassifier(
                "count",
                "location",
                "==",
                "CAN"
        );
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());

        DataFrame outliers = output.filter(
                pc.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == 3);
    }

    @Test
    public void testRange() throws Exception {
        Map<String, Collection<List<String>>> sentinel = new HashMap<>();
        sentinel.put(
                "outlier",
                Arrays.asList(
                        Arrays.asList("CAN"),
                        Arrays.asList("USA", "USB")
                ));
        PredicateCubeClassifier pc = new PredicateCubeClassifier(
                "count",
                "location",
                "str_range",
                sentinel
        );
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());

        DataFrame outliers = output.filter(
                pc.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == 4);
    }

    @Test
    public void testNumRange() throws Exception {
        Map<String, Collection<List<Double>>> sentinel = new HashMap<>();
        sentinel.put(
                "outlier",
                Arrays.asList(
                        Arrays.asList(0.0, 10.0)
                ));
        PredicateCubeClassifier pc = new PredicateCubeClassifier(
                "count",
                "mean",
                "num_range",
                sentinel
        );
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());

        DataFrame outliers = output.filter(
                pc.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == 1);
    }
}