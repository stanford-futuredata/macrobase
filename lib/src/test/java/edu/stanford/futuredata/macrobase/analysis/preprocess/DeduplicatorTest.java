package edu.stanford.futuredata.macrobase.analysis.preprocess;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeduplicatorTest {
    private DataFrame df;

    @Before
    public void setUp() {
        df = new DataFrame();
        double[] a1 = new double[100];
        for (int i = 0; i < a1.length; i++) {
            a1[i] = i;
        }
        df.addDoubleColumn("a1", a1);

        double[] a2 = new double[100];
        for (int i = 0; i < a2.length; i++) {
            a2[i] = (i+10)%100;
        }
        df.addDoubleColumn("a2", a2);

        double[] a3 = new double[100];
        System.arraycopy(a1, 0, a3, 0, a1.length);
        df.addDoubleColumn("a3", a3);

        double[] a4 = new double[100];
        for (int i = 0; i < a4.length; i++) {
            a4[i] = 2*i;
        }
        df.addDoubleColumn("a4", a4);
    }

    @Test
    public void testDeduplicate() throws Exception {
        assertEquals(100, df.getNumRows());
        Deduplicator deduplicator = new Deduplicator();
        DataFrame output = deduplicator.deduplicate(df);
        assertEquals(df.getNumRows(), output.getNumRows());
        System.out.format("Num columns: %d to %d\n",
            df.getSchema().getColumnNamesByType(Schema.ColType.DOUBLE).size(),
            output.getSchema().getNumColumns());
        for (Deduplicator.RemovedColumn col : deduplicator.getRemovedColumns()) {
            System.out.format("\tremoved %s (matched %s with %f similarity)\n", col.removedColumn,
                col.matchedColumn, col.similarity);
        }
    }

    @Test
    public void testDeduplicateHepmass() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        String[] columnNames = new String[27];
        for (int i = 0; i < 27; i++) {
            columnNames[i] = "f" + String.valueOf(i);
            schema.put(columnNames[i], Schema.ColType.DOUBLE);
        }
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/hepmass1k.csv"
        ).setColumnTypes(schema);
        df = loader.load();

        Deduplicator deduplicator = new Deduplicator();
        DataFrame output = deduplicator.deduplicate(df);
        assertEquals(df.getNumRows(), output.getNumRows());
        System.out.format("Num columns: %d to %d\n",
            df.getSchema().getColumnNamesByType(Schema.ColType.DOUBLE).size(),
            output.getSchema().getNumColumns());
        for (Deduplicator.RemovedColumn col : deduplicator.getRemovedColumns()) {
            System.out.format("\tremoved %s (matched %s with %f similarity)\n", col.removedColumn,
                col.matchedColumn, col.similarity);
        }
    }
}