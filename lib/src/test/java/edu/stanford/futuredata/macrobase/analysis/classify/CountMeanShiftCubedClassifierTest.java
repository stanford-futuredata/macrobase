package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CountMeanShiftCubedClassifierTest {

    @Test
    public void testClassifyStrPredicate() throws Exception {
        DataFrame df;
        List<String> requiredColumns = new ArrayList<>(Arrays.asList("time", "location", "version", "count", "meanLatency"));
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put("time", Schema.ColType.STRING);
        colTypes.put("count", Schema.ColType.DOUBLE);
        colTypes.put("meanLatency", Schema.ColType.DOUBLE);
        CSVDataFrameParser loader = new CSVDataFrameParser("src/test/resources/sample_cubedshift.csv", requiredColumns);
        loader.setColumnTypes(colTypes);
        df = loader.load();
        assertEquals(9, df.getNumRows());
        CountMeanShiftCubedClassifier pc = new CountMeanShiftCubedClassifier("count", "time", "meanLatency", "==", "1");
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(5, df.getSchema().getNumColumns());
        assertEquals(9, output.getSchema().getNumColumns());


        double[] outlierCountColumn = output.getDoubleColumnByName(CountMeanShiftCubedClassifier.outlierCountColumnName);
        double[] inlierMeanColumn = output.getDoubleColumnByName(CountMeanShiftCubedClassifier.inlierMeanSumColumnName);

        assertEquals(150, outlierCountColumn[0], 0.1);
        assertEquals(0, outlierCountColumn[1], 0.1);
        assertEquals(0, outlierCountColumn[8], 0.1);
        assertEquals(1, outlierCountColumn[6], 0.1);
        assertEquals(0, inlierMeanColumn[2], 0.1);
        assertEquals(20 * 180, inlierMeanColumn[3], 0.1);
        assertEquals(25 * 200, inlierMeanColumn[5], 0.1);
        assertEquals(100 * 2, inlierMeanColumn[7], 0.1);
    }

    @Test
    public void testClassifyDoublePredicate() throws Exception {
        DataFrame df;
        List<String> requiredColumns = new ArrayList<>(Arrays.asList("time", "location", "version", "count", "meanLatency"));
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put("time", Schema.ColType.DOUBLE);
        colTypes.put("count", Schema.ColType.DOUBLE);
        colTypes.put("meanLatency", Schema.ColType.DOUBLE);
        CSVDataFrameParser loader = new CSVDataFrameParser("src/test/resources/sample_cubedshift.csv", requiredColumns);
        loader.setColumnTypes(colTypes);
        df = loader.load();
        assertEquals(9, df.getNumRows());
        CountMeanShiftCubedClassifier pc = new CountMeanShiftCubedClassifier("count", "time", "meanLatency", "==", 1.0);
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(5, df.getSchema().getNumColumns());
        assertEquals(9, output.getSchema().getNumColumns());


        double[] outlierCountColumn = output.getDoubleColumnByName(CountMeanShiftCubedClassifier.outlierCountColumnName);
        double[] inlierMeanColumn = output.getDoubleColumnByName(CountMeanShiftCubedClassifier.inlierMeanSumColumnName);

        assertEquals(150, outlierCountColumn[0], 0.1);
        assertEquals(0, outlierCountColumn[1], 0.1);
        assertEquals(0, outlierCountColumn[8], 0.1);
        assertEquals(1, outlierCountColumn[6], 0.1);
        assertEquals(0, inlierMeanColumn[2], 0.1);
        assertEquals(20 * 180, inlierMeanColumn[3], 0.1);
        assertEquals(25 * 200, inlierMeanColumn[5], 0.1);
        assertEquals(100 * 2, inlierMeanColumn[7], 0.1);
    }

}
