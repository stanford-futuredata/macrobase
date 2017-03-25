package edu.stanford.futuredata.macrobase.datamodel;

import org.junit.Test;

import static edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SchemaTest {
    @Test
    public void testSimple() throws Exception {
        Schema s = new Schema();
        s.addColumn(ColType.DOUBLE, "usage");
        s.addColumn(ColType.STRING, "app_ver");
        assertTrue(s.getColumnIndex("app_ver") < 2);
        assertEquals(2, s.getNumColumns());
        assertEquals(ColType.DOUBLE, s.getColumnType(s.getColumnIndex("usage")));
    }
}