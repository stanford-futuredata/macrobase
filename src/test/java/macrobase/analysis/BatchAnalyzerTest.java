package macrobase.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.ingest.CsvLoader;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.transform.ZeroToOneLinearTransformation;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

import static org.junit.Assert.*;

public class BatchAnalyzerTest {
    private static final Logger log = LoggerFactory.getLogger(BatchAnalyzerTest.class);

    /*
        N.B. These tests could use considerable love.
             Right now, they basically just catch changed behavior
             in our core analysis pipelines.
     */

    @Test
    public void testMADAnalyzer() throws Exception {
        BatchAnalyzer analyzer = new BatchAnalyzer();
        analyzer.setAlphaMCD(.05);
        analyzer.setTargetPercentile(.99);
        analyzer.forceUsePercentile(true);
        analyzer.setMinInlierRatio(.01);
        analyzer.setMinSupport(.01);
        analyzer.setSeedRand(true);

        CsvLoader loader = new CsvLoader();
        loader.connect("src/test/resources/data/simple.csv");

        AnalysisResult ar = analyzer.analyze(loader,
                                             Lists.newArrayList("A1", "A2", "A3", "A4"),
                                             Lists.newArrayList("A5"),
                                             Lists.newArrayList(),
                                             Lists.newArrayList(),
                                             "",
                                             new ZeroToOneLinearTransformation());

        assertEquals(1, ar.getItemSets().size());

        HashSet<String> toFindColumn = Sets.newHashSet("A1", "A2", "A3", "A4");
        HashSet<String> toFindValue = Sets.newHashSet("0", "1", "2", "3");

        for(ColumnValue cv : ar.getItemSets().get(0).getItems()) {
            assertTrue(toFindColumn.contains(cv.getColumn()));
            toFindColumn.remove(cv.getColumn());
            assertTrue(toFindValue.contains(cv.getValue()));
            toFindValue.remove(cv.getValue());
        }

        assertEquals(0, toFindColumn.size());
        assertEquals(0, toFindValue.size());
    }

    @Test
    public void testMCDAnalyzer() throws Exception {
        BatchAnalyzer analyzer = new BatchAnalyzer();
        analyzer.setAlphaMCD(.05);
        analyzer.setTargetPercentile(.99);
        analyzer.forceUsePercentile(true);
        analyzer.setMinInlierRatio(.01);
        analyzer.setMinSupport(.01);
        analyzer.setSeedRand(true);

        CsvLoader loader = new CsvLoader();
        loader.connect("src/test/resources/data/simple.csv");

        AnalysisResult ar = analyzer.analyze(loader,
                                             Lists.newArrayList("A1", "A2", "A3"),
                                             Lists.newArrayList("A4", "A5"),
                                             Lists.newArrayList(),
                                             Lists.newArrayList(),
                                             "",
                                             new ZeroToOneLinearTransformation());

        assertEquals(1, ar.getItemSets().size());

        HashSet<String> toFindColumn = Sets.newHashSet("A1", "A2", "A3");
        HashSet<String> toFindValue = Sets.newHashSet("0", "1", "2");

        for(ColumnValue cv : ar.getItemSets().get(0).getItems()) {
            assertTrue(toFindColumn.contains(cv.getColumn()));
            toFindColumn.remove(cv.getColumn());
            assertTrue(toFindValue.contains(cv.getValue()));
            toFindValue.remove(cv.getValue());
        }

        assertEquals(0, toFindColumn.size());
        assertEquals(0, toFindValue.size());
    }
}
