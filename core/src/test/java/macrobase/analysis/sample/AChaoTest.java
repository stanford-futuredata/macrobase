package macrobase.analysis.sample;

import com.google.common.collect.Lists;
import macrobase.datamodel.Datum;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;


public class AChaoTest {
    @Test
    public void simpleTest() throws Exception {
        int[] sample = {1, 2, 3, 4, 5, 6, 7};

        Random r = new Random(0);
        AChao<Integer> ac = new AChao<>(2, r);

        for(int i : sample) {
            ac.insert(i, 1);
        }

        assertEquals((Integer) 2, (Integer) ac.getReservoir().size());
        assertEquals((Integer) 5, ac.getReservoir().get(0));
        assertEquals((Integer) 4, ac.getReservoir().get(1));
    }

    @Test
    public void testOverweightItems() throws Exception {
        int[] sample = {1, 2, 3, 4, 5, 6, 7};

        Random r = new Random(0);
        AChao<Integer> ac = new AChao<>(2, r);

        for(int i : sample) {
            ac.insert(i, 1);
        }

        assertEquals((Integer) 2, (Integer) ac.getReservoir().size());
        assertEquals((Integer) 5, ac.getReservoir().get(0));
        assertEquals((Integer) 4, ac.getReservoir().get(1));

        ac.decayWeights(.1);
        ac.insert(100, 1000);

        assertEquals((Integer) 2, (Integer) ac.getReservoir().size());
        assertTrue(ac.getReservoir().contains(100));

        ac.decayWeights(.00001);

        ac.insert(200, 1000);
        assertTrue(ac.getReservoir().contains(200));
    }

    @Test
    public void testOverweightItemSequential() throws Exception {
        int[] sample = {1, 2, 3, 4, 5, 6, 7};

        Random r = new Random(0);
        AChao<Integer> ac = new AChao<>(100, r);

        for(int j = 0; j < 100; ++j) {
            for (int i : sample) {
                ac.insert(i, 1);
            }
        }

        ac.decayWeights(.00001);
        ac.insert(100, 1);
        ac.insert(200, 1);
        ac.insert(300, 1);

        assertEquals((Integer) 100, (Integer) ac.getReservoir().size());
        assertTrue(ac.getReservoir().contains(100));

        ac.decayWeights(.0000001);
        ac.insert(400, 1);

        assertTrue(ac.getReservoir().contains(400));
    }
}
