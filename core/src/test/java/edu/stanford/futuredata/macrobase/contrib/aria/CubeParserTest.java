package edu.stanford.futuredata.macrobase.contrib.aria;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class CubeParserTest {
    @Test
    public void testJsonMapping() throws IOException {
        String path = "src/test/resources/aria_api_cube.json";
        File f = new File(path);
        ObjectMapper mapper = new ObjectMapper();
        APICubeResult cube = mapper.readValue(f, APICubeResult.class);
        assertEquals("PT4H", cube.granularity);
        assertEquals(2, cube.series.size());
        assertEquals(2, cube.series.get(0).filters.size());
    }

    @Test
    public void testJsonToDataFrame() throws IOException {
        String path = "src/test/resources/aria_api_cube.json";
        CubeParser p = CubeParser.loadFromFile(new File(path));
        DataFrame df = p.getDataFrame();
        assertEquals(3, df.getNumRows());
        assertEquals(5,df.getSchema().getNumColumns());
        assertEquals("USA", df.getStringColumnByName("country")[0]);
    }
}