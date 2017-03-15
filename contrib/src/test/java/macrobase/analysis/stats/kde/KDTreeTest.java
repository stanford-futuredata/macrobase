package macrobase.analysis.stats.kde;

import macrobase.util.data.TinyDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;

public class KDTreeTest {
    private static final Logger log = LoggerFactory.getLogger(KDTreeTest.class);
    public static List<double[]> verySimpleData;
    public static KDTree verySimpleTree;

    private static List<double[]> loadVerySimple() throws Exception {
        return new TinyDataSource().get();
    }

    @BeforeClass
    public static void setUp() throws Exception{
        verySimpleData = loadVerySimple();
        verySimpleTree = new KDTree()
                .setLeafCapacity(2)
                .build(verySimpleData);
    }

    @Test
    public void testConstruction() throws Exception {
        KDTree node = verySimpleTree;
        List<double[]> data = verySimpleData;
        assertEquals(0, node.getSplitDimension());
        assertEquals(data.size(), node.getNBelow());

        // Check that points are distributed among children
        KDTree lo = node.getLoChild();
        KDTree hi = node.getHiChild();
        assertEquals(1, lo.getSplitDimension());
        assertEquals(1, lo.getSplitDimension());
        assertEquals(data.size(), lo.getNBelow() + hi.getNBelow());

        double[][] boundaries;
        int dimensions = data.get(0).length;

        // Check boundaries for a normal tree.
        boundaries = node.getBoundaries();
        double[][] actualBoundaries = {
                {1, 11},
                {1, 7},
                {1, 9},
        };
        for (int i=0; i<dimensions; i++) {
            assertArrayEquals(actualBoundaries[i], boundaries[i], 1e-7);
        }

        // Get a Tree that is just a leaf.
        node = new KDTree().setLeafCapacity(50).build(data);
        boundaries = node.getBoundaries();
        for (int i=0; i<dimensions; i++) {
            assertArrayEquals(actualBoundaries[i], boundaries[i], 1e-7);
        }
        assertTrue(node.isLeaf());
    }

    @Test
    public void testEstimateDistances() throws Exception {
        List<double[]> data = verySimpleData;
        KDTree node = verySimpleTree;
        double[] zeroArray = {0, 0, 0};
        for (double[] datum : data) {
            // Check that minimum distance is zero if the point is inside.
            assertArrayEquals(zeroArray, node.getMinMaxDistanceVectors(datum)[0], 1e-7);
            assertEquals(0, node.getMinMaxDistances(datum)[0], 1e-7);
        }

        double[] farAwayPoint = {-10, 5, 21};
        double[] minArray = {11, 0, 12};
        double[] maxArray = {21, 4, 20};
        double[][] minMaxVectors= node.getMinMaxDistanceVectors(farAwayPoint);
        assertArrayEquals(minArray, minMaxVectors[0], 1e-7);
        assertArrayEquals(maxArray, minMaxVectors[1], 1e-7);

        double minDist = 265;
        double maxDist = 857;
        assertEquals(minDist, node.getMinMaxDistances(farAwayPoint)[0], 1e-7);
        assertEquals(maxDist, node.getMinMaxDistances(farAwayPoint)[1], 1e-7);
    }


    @Test
    public void testIsInsideBoundaries() throws Exception {
        List<double[]> data = verySimpleData;
        KDTree node = verySimpleTree;
        for (double[] datum : data) {
            assertTrue(node.isInsideBoundaries(datum));
        }
    }

    @Test
    public void testIsOutsideBoundaries() throws Exception {
        KDTree node = verySimpleTree;

        double[] metrics = {-1, -1, -1};
        assertFalse(node.isInsideBoundaries(metrics));
    }

    @Test
    public void testToString() throws Exception {
        List<double[]> data = verySimpleData;
        KDTree node = verySimpleTree;
        String str = node.toString();
        assertThat(str.length(), greaterThan(Integer.valueOf(data.size())));
    }

    @Test
    public void testEquiWidth() throws Exception {
        double[][] weirdSplitData = {
                {1.0},
                {8.0},
                {9.0},
                {10.0},
                {10.0},
                {10.0},
                {10.0}
        };
        List<double[]> weirdSplit = Arrays.asList(weirdSplitData);

        KDTree node = new KDTree()
                .setLeafCapacity(2)
                .setSplitByWidth(true)
                .build(weirdSplit);

        assertEquals(1, node.getLoChild().getNBelow());
        assertEquals(6, node.getHiChild().getNBelow());

        assertEquals(1, node.getHiChild().getLoChild().getNBelow());
    }

    @Test
    public void testIndices() throws Exception {
        List<double[]> data = verySimpleData;
        KDTree node = verySimpleTree;
        Set<Integer> idxs = new HashSet<>(data.size());
        for (int i : verySimpleTree.idxs) {
            idxs.add(i);
        }
        for (int i=0;i<data.size();i++){
            assertTrue("indices contain "+i, idxs.contains(i));
        }
    }
}
