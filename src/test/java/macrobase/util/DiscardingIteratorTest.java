package macrobase.util;

import macrobase.ingest.DatumEncoder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiscardingIteratorTest {
    @Test
    public void testSimpleDiscard() {
        List<Integer> testSet = new ArrayList<>();
        for(int i = 0; i < 100; ++i) {
            testSet.add(i);

        }

        DiscardingIterator<Integer> d = new DiscardingIterator<>(testSet.iterator(), 99);
        assertTrue(d.hasNext());
        assertEquals((int) d.next(), 99);
        assertFalse(d.hasNext());
    }
}
