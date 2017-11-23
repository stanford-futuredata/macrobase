package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Test;

import static org.junit.Assert.*;

public class CSVDataFrameWriterTest {
    @Test
    public void writeToStream() throws Exception {
        StringBuilder sb = new StringBuilder();
        DataFrame df = new DataFrame();
        String[] col1 = {"a", "b"};
        double[] col2 = {1.0, 2.0};
        df.addStringColumn("col1", col1);
        df.addDoubleColumn("col2", col2);

        CSVDataFrameWriter writer = new CSVDataFrameWriter();
        writer.writeToStream(df, sb);
        String out = sb.toString();
        assertEquals(3, out.split("\n").length);
        assertTrue(out.contains("b"));
    }
}