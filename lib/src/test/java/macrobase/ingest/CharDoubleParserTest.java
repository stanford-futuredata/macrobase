package macrobase.ingest;

import org.junit.Test;

import static org.junit.Assert.*;

public class CharDoubleParserTest {
    @Test
    public void parseDouble() throws Exception {
        CharDoubleParser p = new CharDoubleParser();
        String s = "3.14";
        assertEquals(3.14, p.parseDouble(s.toCharArray(), 0, s.length()), 1e-10);

        s = "-144 0,2,";
        assertEquals(-1440.0, p.parseDouble(s.toCharArray(), 0, 6), 1e-10);
        assertEquals(2.0, p.parseDouble(s.toCharArray(), 7, 8), 1e-10);
        assertEquals(Double.NaN, p.parseDouble(s.toCharArray(), 9, 9), 1e-10);
    }

}