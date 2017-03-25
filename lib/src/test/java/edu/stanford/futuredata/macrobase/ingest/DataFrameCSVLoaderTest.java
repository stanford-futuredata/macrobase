package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataFrameCSVLoaderTest {
    @Test
    public void testLoadSimple() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put("usage", Schema.ColType.DOUBLE);

        DataFrameLoader loader = new CSVDataFrameLoader("src/test/resources/tiny.csv")
                .setColumnTypes(colTypes);
        DataFrame df = loader.load();

        assertEquals(3, df.getNumRows());
        assertEquals(3, df.getSchema().getNumColumns());
        double[] usage = df.getDoubleColumnByName("usage");
        assertEquals(usage[0], 2.0, 1e-10);

        Row row = df.getRow(1);
        assertEquals("CAN", row.getAs("location"));
    }
}