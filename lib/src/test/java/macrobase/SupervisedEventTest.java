package macrobase;

import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.itemset.ItemsetEncoder;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.datamodel.DataFrame;
import macrobase.datamodel.Row;
import macrobase.datamodel.Schema;
import org.junit.*;

import java.util.*;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;

public class SupervisedEventTest {
    private List<Map<String, Object>> events;

    @Before
    public void setUp() {
        events = new ArrayList<>();
        for (int i = 0; i < 900; i++) {
            Map<String, Object> event = new HashMap<>();
            event.put("serverID", "s"+(i%20));
            event.put("region", "r"+(i%7));
            event.put("sev", "debug");
            events.add(event);
        }
        for (int i = 0; i < 100; i++) {
            Map<String, Object> event = new HashMap<>();
            event.put("serverID", "s"+(i%2));
            event.put("region", "r3");
            event.put("sev", "error");
            events.add(event);
        }
        Collections.shuffle(events);
    }

    class Explainer {
        public List<String> attributes;
        public Predicate<Map<String, Object>> isOutlier;
        public static final String outlierColumn = "_OUTLIER";

        public boolean useAttributeCombinations = true;
        public double minSupport = 0.05;
        public double minIORatio = 3.0;

        public Explainer(List<String> attributes, Predicate<Map<String, Object>> isOutlier) {
            this.attributes = attributes;
            this.isOutlier = isOutlier;
        }
        public Explainer setUseAttributeCombinations(boolean flag) {
            this.useAttributeCombinations = flag;
            return this;
        }
        public Explainer setMinSupport(double minSupport) {
            this.minSupport = minSupport;
            return this;
        }
        public Explainer setMinIORatio(double minIORatio) {
            this.minIORatio = minIORatio;
            return this;
        }

        public DataFrame prepareBatch(List<Map<String, Object>> events) {
            int n = events.size();
            Schema schema = new Schema();
            schema.addColumn(Schema.ColType.DOUBLE, Explainer.outlierColumn);
            for (String attr: attributes) {
                schema.addColumn(Schema.ColType.STRING, attr);
            }

            List<Row> rows = new ArrayList<>(n);
            for (Map<String, Object> event : events) {
                List<Object> fields = new ArrayList<>();
                fields.add(isOutlier.test(event) ? 1.0 : 0.0);
                for (String attr: attributes) {
                    fields.add(event.getOrDefault(attr, "MISSING").toString());
                }
                rows.add(new Row(fields));
            }

            DataFrame df = new DataFrame().loadRows(schema, rows);
            return df;
        }

        public Summary predictBatch(DataFrame batch) {
            BatchSummarizer summ = new BatchSummarizer();
            summ.setUseAttributeCombinations(true);
            summ.setAttributes(attributes);
            summ.process(batch);

            return summ.getResults();
        }
        public Summary getResults(List<Map<String, Object>> events) {
            return predictBatch(prepareBatch(events));
        }
    }

    @Test
    public void testGetSummaries() {
        List<String> attributes = Arrays.asList("serverID", "region");
        Explainer e = new Explainer(attributes, event -> "error".equals(event.get("sev")));
        DataFrame df = e.prepareBatch(events);
        assertEquals(1000,df.getNumRows());

        Summary s = e.predictBatch(df);
        assertEquals(100, s.getNumOutliers());
        assertEquals(900, s.getNumInliers());
        List<ItemsetResult> is = s.getItemsets();
        assertEquals(5, is.size());
        int numSingleton = 0;
        for (ItemsetResult itemResult : is) {
            Map<String, String> curItems = itemResult.getItems();
            if (curItems.size() == 1) {
                numSingleton++;
            }
        }
        assertEquals(3, numSingleton);
    }
}
