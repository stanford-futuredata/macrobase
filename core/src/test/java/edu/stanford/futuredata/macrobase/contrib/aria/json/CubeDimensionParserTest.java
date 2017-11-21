package edu.stanford.futuredata.macrobase.contrib.aria.json;

import edu.stanford.futuredata.macrobase.contrib.aria.json.CubeDimensionParser;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CubeDimensionParserTest {
    @Test
    public void testParseSimple() throws IOException {
        String path = "src/test/resources/aria_dimensions.json";
        File f = new File(path);
        CubeDimensionParser p = CubeDimensionParser.loadFromFile(f);
        Map<String, List<String>> dimValues = p.getDimensionValues();
        assertEquals(2, dimValues.size());
    }

}