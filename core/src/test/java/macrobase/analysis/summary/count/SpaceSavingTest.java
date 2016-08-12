package macrobase.analysis.summary.count;

/**
 * Created by pbailis on 12/24/15.
 */

import macrobase.analysis.summary.count.SpaceSavingList;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SpaceSavingTest {

    @Test
    public void simpleTest() {
        SpaceSavingList ss = new SpaceSavingList(10);
        ss.observe(1);
        ss.observe(1);
        ss.observe(1);
        ss.observe(2);
        ss.observe(3);
        ss.observe(1);
        ss.observe(3);
        ss.observe(2);
        ss.observe(3);

        assertEquals(4, ss.getCount(1), 0);
        assertEquals(2, ss.getCount(2), 0);
        assertEquals(3, ss.getCount(3), 0);
    }

    @Test
    public void overflowTest() {
        SpaceSavingList ss = new SpaceSavingList(10);

        for (int i = 0; i < 10; ++i) {
            ss.observe(i);
            assertEquals(1, ss.getCount(i), 0);
        }

        ss.observe(10);
        assertEquals(2, ss.getCount(10), 0);
    }

    @Test
    public void decayTest() {
        final int N = 10000;
        final int ITEMS = 20;
        final double DECAY = .5;
        final int CAPACITY = 12;
        final double EPSILON = 1.0/CAPACITY;

        SpaceSavingList ss = new SpaceSavingList(CAPACITY);

        Random r = new Random(0);

        Map<Integer, Double> trueCnt = new HashMap<>();

        for (int i = 0; i < N; ++i) {
            int item = r.nextInt(ITEMS);
            double cnt = r.nextDouble();
            ss.observe(item, cnt);

            trueCnt.compute(item, (k, v) -> v == null ? cnt : v + cnt);

            if (i % 20 == 0) {
                ss.multiplyAllCounts(DECAY);

                trueCnt.forEach((k, v) -> trueCnt.put(k, v*DECAY));
            }
        }

        for (Map.Entry<Integer, Double> cnts : ss.getCounts().entrySet()) {
            assertEquals(trueCnt.get(cnts.getKey()), cnts.getValue(), N*EPSILON);
            assertEquals(trueCnt.get(cnts.getKey()), ss.getCount(cnts.getKey()), N*EPSILON);
        }

        ss.debugPrint();
    }
}
