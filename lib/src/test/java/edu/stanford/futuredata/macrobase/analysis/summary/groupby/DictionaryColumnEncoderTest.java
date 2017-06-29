package edu.stanford.futuredata.macrobase.analysis.summary.groupby;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class DictionaryColumnEncoderTest {
    @Test
    public void testEncodeColumn() {
        String[] col = {"a", "b", "c", "cd", "cd", "a"};
        DictionaryColumnEncoder ec = new DictionaryColumnEncoder();
        int[] eCol = ec.encodeColumn(col);

        int[] expectedValues = {0,1,2,3,3,0};
        assertArrayEquals(expectedValues, eCol);
    }
}