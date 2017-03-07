package macrobase.datamodel;

import org.junit.Test;

import static org.junit.Assert.*;
import static macrobase.datamodel.Schema.ColType;

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