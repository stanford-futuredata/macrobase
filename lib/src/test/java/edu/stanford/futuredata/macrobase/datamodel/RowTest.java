package edu.stanford.futuredata.macrobase.datamodel;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowTest {
    @Test
    public void testSimple() {
        Row row = new Row(Arrays.asList(5.0, "java"));
        assertEquals(5.0, row.getAs(0), 1e-10);
        assertEquals("java", row.getAs(1));
        assertTrue(row.toString().contains(Row.DOUBLE_FORMAT.format(5.0)));

        Schema schema = new Schema();
        schema.addColumn(Schema.ColType.DOUBLE, "metric");
        schema.addColumn(Schema.ColType.STRING, "attribute");
        row = new Row(schema, Arrays.asList(5.0, "java"));
        assertEquals(5.0, row.getAs("metric"), 1e-10);
        assertEquals("java", row.getAs("attribute"));
    }

    @Test
    public void testCompare() {
        Row row1 = new Row(Arrays.asList(5.0, "java"));

        Schema schema = new Schema();
        schema.addColumn(Schema.ColType.DOUBLE, "metric");
        schema.addColumn(Schema.ColType.STRING, "attribute");
        Row row2 = new Row(schema, Arrays.asList(5.0, "java"));

        assertEquals(row1, row2);
        assertEquals(row1.hashCode(), row2.hashCode());
        assertTrue(row2.toString().contains(row1.toString()));
    }

}