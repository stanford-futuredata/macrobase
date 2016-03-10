package macrobase.datamodel;

import com.google.common.collect.Lists;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.CSVIngester;
import macrobase.ingest.DataIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class KDTreeTest {
    private static final Logger log = LoggerFactory.getLogger(KDTreeTest.class);

    private DataIngester loader;
    private List<Datum> data;

    private void setUpSimpleCsv() throws IOException, ConfigurationException, SQLException {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");
        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A2", "A5"));
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, Lists.newArrayList("A1", "A3", "A4"));
        loader = new CSVIngester(conf);
        data = Lists.newArrayList(loader);
    }

    private void setUpVerySimpleCsv() throws IOException, ConfigurationException, SQLException {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/verySimple.csv");
        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("x", "y", "z"));
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, Lists.newArrayList("x", "y", "z"));
        loader = new CSVIngester(conf);
        data = Lists.newArrayList(loader);
    }

    @Test
    public void testConstruction() throws IOException, SQLException, ConfigurationException {
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
        int dimensions = data.get(0).getMetrics().getDimension();

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
    public void testEstimateDistanceVectors() throws IOException, SQLException, ConfigurationException {
        this.setUpVerySimpleCsv();
        KDTree node;
        node = new KDTree(data, 2);
        double[] zeroArray = {0, 0, 0};
        for (Datum datum : data) {
            // Check that minimum distance is zero if the point is inside.
            assertArrayEquals(zeroArray, node.getMinMaxDistanceVectors(datum).get(0).toArray(), 1e-7);
        }

        double[] farAwayPoint = {-10, 5, 21};
        Datum datum = new Datum(new ArrayList<>(), new ArrayRealVector(farAwayPoint));
        double[] minArray = {11, 0, 12};
        double[] maxArray = {21, 4, 20};
        List<RealVector> minMaxVectors= node.getMinMaxDistanceVectors(datum);
        assertArrayEquals(minArray, minMaxVectors.get(0).toArray(), 1e-7);
        assertArrayEquals(maxArray, minMaxVectors.get(1).toArray(), 1e-7);
    }

    @Test
    public void testEstimateDistanceSquared() throws IOException, SQLException, ConfigurationException {
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
    public void testIsInsideBoundaries() throws IOException, SQLException, ConfigurationException {
        this.setUpVerySimpleCsv();
        KDTree node;

        node = new KDTree(data, 2);
        for (Datum datum : data) {
            assertTrue(node.isInsideBoundaries(datum));
        }
    }

    @Test
    public void testPooledCovariance() throws IOException, SQLException, ConfigurationException {
        this.setUpSimpleCsv();
        KDTree node;

        node = new KDTree(data, 200);
        assertTrue(node.isLeaf());

    }

}
