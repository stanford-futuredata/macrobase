package macrobase.analysis.stats.kde.grid;

import macrobase.analysis.stats.kde.kernel.GaussianKernel;
import macrobase.analysis.stats.kde.kernel.Kernel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SoftGridCutoffTest {
    @Test
    public void testSimpleGrid() {
        double[] bw = {1.0, 1.0};
        Kernel k = new GaussianKernel().initialize(bw);

        SoftGridCutoff gc = new SoftGridCutoff(
                k,
                bw,
                1.0
        );
        double[] p = {0.0, 0.1};
        int N=10;
        for (int i=0; i<N;i++) {
            gc.add(p);
        }
        assertEquals(N*k.density(bw), gc.getUnscaledDenseValue(p), 1e-10);

        double[] neighbor = {1.5, 0.5};
        double[] neighbordist = {1.0, 2.0};
        assertEquals(N*k.density(neighbordist), gc.getUnscaledDenseValue(neighbor), 1e-10);

        gc.prune();
        // Should be no more grid cells after pruning
        assertEquals(0, gc.getNumCells());
    }
}
