package macrobase.analysis.index;

import com.google.common.collect.Lists;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import macrobase.ingest.DataIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;

public class KDTreeTest {
    private static final Logger log = LoggerFactory.getLogger(KDTreeTest.class);

    private DataIngester loader;
    private List<Datum> data;

    private void setUpSimpleCsv() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");
        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A2", "A5"));
        conf.set(MacroBaseConf.METRICS, Lists.newArrayList("A1", "A3", "A4"));
        loader = new CSVIngester(conf);
        data = loader.getStream().drain();
    }


    private void setUpVerySimpleCsv() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/verySimple.csv");
        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("x", "y", "z"));
        conf.set(MacroBaseConf.METRICS, Lists.newArrayList("x", "y", "z"));
        loader = new CSVIngester(conf);
        data = loader.getStream().drain();
    }

    @Test
    public void testConstruction() throws Exception {
        this.setUpVerySimpleCsv();
        KDTree node;

        node = new KDTree(data, 2);
        assertEquals(0, node.getSplitDimension());
        // Check that tree depth is 4
        for (int i=0 ; i < 3; i++) {
            node = node.getHiChild();
        }
        assertTrue(node.isLeaf());
        assertEquals(2, node.getnBelow());


        node = new KDTree(data, 3);
        // Check that next split dimension is 2 for loChild
        node = node.getLoChild();
        assertEquals(2, node.getSplitDimension());
        // Check that tree depth is 2 for lo/lo split
        node = node.getLoChild();
        assertTrue(node.isLeaf());
        assertEquals(3, node.getnBelow());


        double[][] boundaries;
        int dimensions = data.get(0).metrics().getDimension();

        // Check boundaries for a normal tree.
        node = new KDTree(data, 4);
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
        node = new KDTree(data, 50);
        boundaries = node.getBoundaries();
        for (int i=0; i<dimensions; i++) {
            assertArrayEquals(actualBoundaries[i], boundaries[i], 1e-7);
        }

        log.debug("{}", node.toString());
    }

    @Test
    public void testEstimateDistanceVectors() throws Exception {
        this.setUpVerySimpleCsv();
        KDTree node;
        node = new KDTree(data, 2);
        double[] zeroArray = {0, 0, 0};
        for (Datum datum : data) {
            // Check that minimum distance is zero if the point is inside.
            assertArrayEquals(zeroArray, node.getMinMaxDistanceVectors(datum)[0].toArray(), 1e-7);
        }

        double[] farAwayPoint = {-10, 5, 21};
        Datum datum = new Datum(new ArrayList<>(), new ArrayRealVector(farAwayPoint));
        double[] minArray = {11, 0, 12};
        double[] maxArray = {21, 4, 20};
        RealVector[] minMaxVectors= node.getMinMaxDistanceVectors(datum);
        assertArrayEquals(minArray, minMaxVectors[0].toArray(), 1e-7);
        assertArrayEquals(maxArray, minMaxVectors[1].toArray(), 1e-7);
    }

    @Test
    public void testEstimateDistanceSquared() throws Exception {
        this.setUpVerySimpleCsv();
        KDTree node;
        node = new KDTree(data, 2);
        for (Datum datum : data) {
            // Check that minimum distance is zero if the point is inside.
            assertEquals(0, node.estimateL2DistanceSquared(datum)[0], 1e-7);
        }

        double[] farAwayPoint = {-10, 5, 21};
        double[] distancesSquaredFromFarAwayPoint = {265, 857};
        Datum datum = new Datum(new ArrayList<>(), new ArrayRealVector(farAwayPoint));
        assertArrayEquals(distancesSquaredFromFarAwayPoint, node.estimateL2DistanceSquared(datum), 1e-7);
    }


    @Test
    public void testIsInsideBoundaries() throws Exception {
        this.setUpVerySimpleCsv();
        KDTree node;

        node = new KDTree(data, 2);
        for (Datum datum : data) {
            assertTrue(node.isInsideBoundaries(datum));
        }
    }

    @Test
    public void testIsOutsideBoundaries() throws Exception {
        this.setUpVerySimpleCsv();
        KDTree node = new KDTree(data, 2);

        double[] metrics = {-1, -1, -1};
        Datum newdatum = new Datum(data.get(0), new ArrayRealVector(metrics));
        assertFalse(node.isInsideBoundaries(newdatum));
    }

    @Test
    public void testToString() throws Exception {
        setUpVerySimpleCsv();
        KDTree node = new KDTree(data, 2);
        String str = node.toString();
        assertThat(str.length(), greaterThan(Integer.valueOf(data.size())));
    }

    @Test
    public void testPooledCovariance() throws Exception {
        this.setUpSimpleCsv();
        KDTree node;

        node = new KDTree(data, 200);
        assertTrue(node.isLeaf());
    }

}
