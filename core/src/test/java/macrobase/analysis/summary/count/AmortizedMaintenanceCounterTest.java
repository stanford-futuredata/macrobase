package macrobase.analysis.summary.count;


import com.google.common.base.Stopwatch;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class AmortizedMaintenanceCounterTest {
    private static final Logger log = LoggerFactory.getLogger(AmortizedMaintenanceCounterTest.class);


    @Test
    public void simpleTest() {
        AmortizedMaintenanceCounter ss = new AmortizedMaintenanceCounter(10);
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
        AmortizedMaintenanceCounter ss = new AmortizedMaintenanceCounter(10);

        for (int i = 0; i < 10; ++i) {
            ss.observe(i);
            assertEquals(1, ss.getCount(i), 0);
        }

        ss.observe(10);
        assertEquals(1, ss.getCount(10), 0);
    }

    @Test
    public void decayTest() {
        final int N = 1000;
        final int ITEMS = 100;
        final double DECAY = .5;
        final int CAPACITY = 15;
        final double EPSILON = 1.0/CAPACITY;

        AmortizedMaintenanceCounter ss = new AmortizedMaintenanceCounter(CAPACITY);

        Random r = new Random(0);

        Map<Integer, Double> trueCnt = new HashMap<>();

        for (int i = 0; i < N; ++i) {
            int item = r.nextInt(ITEMS);
            ss.observe(item);

            trueCnt.compute(item, (k, v) -> v == null ? 1 : v + 1);

            if (i % 10 == 0) {
                ss.multiplyAllCounts(DECAY);

                trueCnt.forEach((k, v) -> trueCnt.put(k, v*DECAY));
            }
        }

        Map<Integer, Double> cnts = ss.getCounts();

        for (Map.Entry<Integer, Double> cnt : cnts.entrySet()) {
            assertEquals(trueCnt.get(cnt.getKey()), cnt.getValue(), N*EPSILON);
        }

        int key = cnts.keySet().iterator().next();
        assertEquals(cnts.get(key), ss.getCount(key), 1e-10);
    }
}
