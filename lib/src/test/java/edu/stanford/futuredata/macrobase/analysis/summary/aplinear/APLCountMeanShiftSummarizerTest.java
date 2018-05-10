package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.classify.CountMeanShiftCubedClassifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class APLCountMeanShiftSummarizerTest  {
    private DataFrame df;

    @Before
    public void setUp() throws Exception {
        List<String> requiredColumns = new ArrayList<>(Arrays.asList("time", "location", "version", "count", "language", "meanLatency"));
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put("time", Schema.ColType.STRING);
        colTypes.put("count", Schema.ColType.DOUBLE);
        colTypes.put("meanLatency", Schema.ColType.DOUBLE);
        CSVDataFrameParser loader = new CSVDataFrameParser("src/test/resources/sample_cubedshift.csv", requiredColumns);
        loader.setColumnTypes(colTypes);
        df = loader.load();
    }

    @Test
    public void testSummarize() throws Exception {
        assertEquals(9, df.getNumRows());
        CountMeanShiftCubedClassifier pc = new CountMeanShiftCubedClassifier("count", "time", "meanLatency", "==", "1");
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());

        List<String> explanationAttributes = Arrays.asList(
                "location",
                "version",
                "language"
        );

        APLCountMeanShiftSummarizer summ = new APLCountMeanShiftSummarizer();
        summ.setMinSupport(.05);
        summ.setMinMeanShift(1.1);
        summ.setAttributes(explanationAttributes);
        summ.process(output);
        APLExplanation e = summ.getResults();
        TestCase.assertEquals(3, e.getResults().size());
        assertTrue(e.prettyPrint().contains("location=AUS"));
        assertTrue(e.prettyPrint().contains("version=v2"));
        assertTrue(e.prettyPrint().contains("language=ENG"));
    }

}

