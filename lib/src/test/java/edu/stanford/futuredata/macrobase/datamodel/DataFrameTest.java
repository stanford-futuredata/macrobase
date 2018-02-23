package edu.stanford.futuredata.macrobase.datamodel;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DataFrameTest {
    private DataFrame tinyDF;

    @Before
    public void setUp() {
        tinyDF = new DataFrame();
        double[] metric = {1.0, 2.0, 3.0};
        String[] attribute = {"a", "a", "b"};
        tinyDF.addColumn("metric", metric);
        tinyDF.addColumn("attribute", attribute);
    }

    @Test
    public void testCreate() {
        assertEquals(3, tinyDF.getNumRows());
        String[] attrColumn = tinyDF.getStringColumnByName("attribute");
        assertEquals(3, attrColumn.length);
        assertEquals("a", attrColumn[0]);
        Row curRow = tinyDF.getRow(0);
        assertEquals(1.0, curRow.getAs("metric"), 1e-10);
    }


    @Test
    public void testBulkOperations() {
        DataFrame selected = tinyDF.project(Arrays.asList("attribute"));
        assertEquals(1, selected.getSchema().getNumColumns());
        DataFrame filtered = selected.filter(
                "attribute",
                (Object a) -> a.equals("a")
        );
        assertEquals(2, filtered.getNumRows());

        filtered = tinyDF.filter(1, (double d) -> d > 2.1);
        assertEquals(1, filtered.getNumRows());

        DataFrame combined = DataFrame.unionAll(
                Arrays.asList(tinyDF, tinyDF, tinyDF)
        );
        assertEquals(tinyDF.getNumRows()*3, combined.getNumRows());
    }

    @Test
    public void testComplexDataFrame() {
        DataFrame df = new DataFrame();
        int n = 100;
        int counter = 0;
        for (int j = 0; j < 5; j++) {
            if (j % 2 == 0) {
                double[] newCol = new double[n];
                for (int i = 0; i < n; i++) {
                    newCol[i] = counter;
                    counter++;
                }
                df.addColumn("d"+j,newCol);
            } else {
                String[] newCol = new String[n];
                for (int i = 0; i < n; i++) {
                    newCol[i] = String.valueOf(counter);
                    counter++;
                }
                df.addColumn("s"+j,newCol);
            }
        }

        DataFrame df2 = DataFrame.unionAll(Arrays.asList(df, df));
        assertEquals(2*n, df2.getNumRows());
        assertEquals(df2.getRow(0), df2.getRow(n));
        assertEquals(2.0 * n, df2.getDoubleColumn(2)[0], 1e-10);
    }
}