package macrobase.datamodel;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class DataFrameTest {
    private DataFrame tinyDF;

    @Before
    public void setUp() {
        tinyDF = new DataFrame(new Schema());
        double[] metric = {1.0, 2.0, 3.0};
        String[] attribute = {"a", "a", "b"};
        tinyDF.addDoubleColumn("metric", metric);
        tinyDF.addStringColumn("attribute", attribute);
    }

    @Test
    public void testCreate() throws Exception {
        assertEquals(3, tinyDF.getNumRows());
        String[] attrColumn = tinyDF.getStringColumnByName("attribute");
        assertEquals(3, attrColumn.length);
        assertEquals("a", attrColumn[0]);
        Row curRow = tinyDF.getRow(0);
        assertEquals(1.0, curRow.getAs("metric"), 1e-10);
    }


    @Test
    public void testBulkOperations() {
        DataFrame selected = tinyDF.selectByName(Arrays.asList("attribute"));
        assertEquals(1, selected.getSchema().getNumColumns());
        DataFrame filtered = selected.filterStringByName(
                "attribute",
                (String a) -> a.equals("a")
        );
        assertEquals(2, filtered.getNumRows());

        filtered = tinyDF.filterDouble(1, (double d) -> d > 2.1);
        assertEquals(1, filtered.getNumRows());
    }
}