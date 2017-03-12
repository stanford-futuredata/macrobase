package macrobase.ingest;

import macrobase.datamodel.DataFrame;
import macrobase.datamodel.Row;
import macrobase.datamodel.Schema;
import macrobase.ingest.FastDataFrameCSVLoader;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FastDataFrameCSVLoaderTest {
    @Test
    public void testLoadSimple() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put("usage", Schema.ColType.DOUBLE);

        DataFrameCSVLoader loader = new FastDataFrameCSVLoader("src/test/resources/tiny.csv")
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