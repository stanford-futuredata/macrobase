package macrobase;

import macrobase.analysis.summary.itemset.ItemsetEncoder;
import macrobase.analysis.summary.OutlierGroup;
import macrobase.datamodel.DataFrame;
import macrobase.datamodel.Row;
import macrobase.datamodel.Schema;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class SupervisedEventTest {
    @Before
    public void setUp() {

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

        public List<OutlierGroup> predictBatch(DataFrame batch) {
            // TODO: support int columns that automatically filter based on 0/1
            // TODO: support object columns so we can store itemsets in dataframe and then filter
            DataFrame outlierDF = batch.filterDoubleByName(
                    Explainer.outlierColumn,
                    d -> d > 0.0
            );
            DataFrame inlierDF = batch.filterDoubleByName(
                    Explainer.outlierColumn,
                    d -> d == 0.0
            );

            ItemsetEncoder encoder = new ItemsetEncoder();
            List<Set<Integer>> inlierItemsets = encoder.encodeColumns(inlierDF.getStringColsByName(attributes));
            List<Set<Integer>> outlierItemsets = encoder.encodeColumns(outlierDF.getStringColsByName(attributes));

            // TODO: fill in call to summarizer
            List<OutlierGroup> result = null;

            return result;
        }
        public List<OutlierGroup> getResults(List<Map<String, Object>> events) {
            return predictBatch(prepareBatch(events));
        }
    }

    @Test
    public void  testGetSummaries() {

    }
}
