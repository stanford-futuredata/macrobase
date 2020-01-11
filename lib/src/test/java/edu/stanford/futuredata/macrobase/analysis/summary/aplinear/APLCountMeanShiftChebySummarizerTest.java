package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.classify.CountMeanShiftChebyClassifier;
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

public class APLCountMeanShiftChebySummarizerTest {
    private DataFrame df;

    @Before
    public void setUp() throws Exception {
        List<String> requiredColumns = new ArrayList<>(Arrays.asList("t", "d1", "d2", "x"));
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put("t", Schema.ColType.STRING);
        colTypes.put("d1", Schema.ColType.STRING);
        colTypes.put("d2", Schema.ColType.STRING);
        colTypes.put("x", Schema.ColType.DOUBLE);
        CSVDataFrameParser loader = new CSVDataFrameParser(
                "/Users/edgan/Documents/projects/macrobase/tools/synth_cluster_tight.csv",
                requiredColumns
        );
        loader.setColumnTypes(colTypes);
        df = loader.load();
    }

    @Test
    public void testSummarize() throws Exception {
        assertEquals(10000, df.getNumRows());
        CountMeanShiftChebyClassifier pc = new CountMeanShiftChebyClassifier(
                "t", "x", "==", "1");
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());

        List<String> explanationAttributes = Arrays.asList(
                "d1",
                "d2"
        );

        APLCountMeanShiftChebySummarizer summ = new APLCountMeanShiftChebySummarizer();
        summ.setMinSupport(.02);
        summ.setMinMeanShift(4.0);
        summ.setAttributes(explanationAttributes);
        summ.process(output);
        APLExplanation e = summ.getResults();
        System.out.println(e.prettyPrint());
//        TestCase.assertEquals(3, e.getResults().size());
//        assertTrue(e.prettyPrint().contains("location=AUS"));
//        assertTrue(e.prettyPrint().contains("version=v2"));
//        assertTrue(e.prettyPrint().contains("language=ENG"));
    }

}

