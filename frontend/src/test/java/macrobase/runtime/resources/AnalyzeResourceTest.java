package macrobase.runtime.resources;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.result.ColumnValue;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnalyzeResourceTest {
    @Test
    public void testMADAnalyzer() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.99) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");

        conf.loadSystemProperties();

        AnalyzeResource resource = new AnalyzeResource(conf);
        AnalyzeResource.AnalysisRequest r = new AnalyzeResource.AnalysisRequest();
        r.attributes = Lists.newArrayList("A1", "A2", "A3", "A4");
        r.highMetrics = Lists.newArrayList();
        r.lowMetrics = Lists.newArrayList("A5");

        // this is not a great API...
        r.pgUrl = "should be unused";
        r.baseQuery = "also should be unused";

        List<AnalysisResult> arl = resource.getAnalysis(r).results;

        assertTrue(arl.size() == 1);

        AnalysisResult ar = arl.get(0);

        assertEquals(1, ar.getItemSets().size());

        HashSet<String> toFindColumn = Sets.newHashSet("A1", "A2", "A3", "A4");
        HashSet<String> toFindValue = Sets.newHashSet("0", "1", "2", "3");

        for (ColumnValue cv : ar.getItemSets().get(0).getItems()) {
            assertTrue(toFindColumn.contains(cv.getColumn()));
            toFindColumn.remove(cv.getColumn());
            assertTrue(toFindValue.contains(cv.getValue()));
            toFindValue.remove(cv.getValue());
        }

        assertEquals(0, toFindColumn.size());
        assertEquals(0, toFindValue.size());
    }
}
