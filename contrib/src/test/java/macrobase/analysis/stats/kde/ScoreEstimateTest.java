package macrobase.analysis.stats.kde;

import macrobase.analysis.stats.kde.kernel.GaussianKernel;
import macrobase.analysis.stats.kde.kernel.Kernel;
import macrobase.util.data.TinyDataSource;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ScoreEstimateTest {
    @Test
    public void testSplitMonotonic() throws Exception {
        List<double[]> data = new TinyDataSource().get();
        KDTree tree = new KDTree().setLeafCapacity(2).build(data);
        double[] bw = {3.0, 3.0, 3.0};
        Kernel k = new GaussianKernel().initialize(bw);

        double[] pt = {5.0, 5.0, 5.0};
        ScoreEstimate s = new ScoreEstimate(k, tree, pt);
        ScoreEstimate[] children = s.split(k, pt);
        for (ScoreEstimate c : children) {
            assertThat(c.wMax, lessThanOrEqualTo(s.wMax));
            assertThat(c.wMin, greaterThanOrEqualTo(s.wMin));
        }

        assertEquals(
                s.tree.getNBelow(),
                children[0].tree.getNBelow()+children[1].tree.getNBelow()
        );
        assertThat(
                s.totalWMax,
                greaterThan(children[0].totalWMax+children[1].totalWMax)
        );
        assertThat(
                s.totalWMin,
                lessThan(children[0].totalWMin+children[1].totalWMin)
        );
    }
}
