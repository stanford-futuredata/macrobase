package macrobase.analysis.stats.kde.grid;

import macrobase.analysis.stats.kde.SimpleKDE;
import macrobase.analysis.stats.kde.kernel.GaussianKernel;
import macrobase.analysis.stats.kde.kernel.Kernel;
import macrobase.util.data.TinyDataSource;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class CompositeGridTest {
    @Test
    public void testSimpleGrid() {
        List<double[]> data = new TinyDataSource().get();
        double[] bw = {2,2,2};
        double[] zeros = {0,0,0};
        Kernel k = new GaussianKernel().initialize(bw);
        SimpleKDE kde = new SimpleKDE()
                .setBandwidth(bw)
                .setIgnoreSelf(true)
                .train(data);

        double cutoff = 2e-4;
        CompositeGrid grid = new CompositeGrid(
                k,
                bw,
                Arrays.asList(1.0),
                cutoff
        ).train(data);

        for (double[] d : data) {
            double kdeDensity = kde.density(d);
            double gridDensity = grid.density(d);
            assertThat(gridDensity, lessThanOrEqualTo(kdeDensity));
            if (gridDensity > 0) {
                assertThat(gridDensity, greaterThan(cutoff));
            }
        }
    }
}
