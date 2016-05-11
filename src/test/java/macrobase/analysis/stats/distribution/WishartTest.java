package macrobase.analysis.stats.distribution;

import org.apache.commons.math3.linear.BlockRealMatrix;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.TestCase.assertEquals;

public class WishartTest {
    private static final Logger log = LoggerFactory.getLogger(WishartTest.class);

    @Test
    public void entropyTest() {
        double[][] matrixArray;

        // Comparison values were taken from scipy implementation of wishart entropy.
        matrixArray = new double[][]{{1, 0}, {0, 1}};
        assertEquals(3.953808582067758, new Wishart(new BlockRealMatrix(matrixArray), 2).getEntropy(), 1e-7);

        matrixArray = new double[][]{{2, 1}, {1, 2}};
        assertEquals(8.4584668123359084, new Wishart(new BlockRealMatrix(matrixArray), 5).getEntropy(), 1e-7);
    }
}
