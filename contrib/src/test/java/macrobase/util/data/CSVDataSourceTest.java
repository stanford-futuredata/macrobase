package macrobase.util.data;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CSVDataSourceTest {
    @Test
    public void testLoadFile() throws Exception {
        DataSource in = new CSVDataSource(
                "src/test/resources/data/simple.csv",
                Arrays.asList(0, 1)
        );
        List<double[]> metrics = in.get();

        assertEquals(100, metrics.size());
        assertEquals(2, metrics.get(1).length);
        assertEquals(37.0, metrics.get(1)[0], 1e-10);
        assertEquals(38.0, metrics.get(1)[1], 1e-10);
    }
}
