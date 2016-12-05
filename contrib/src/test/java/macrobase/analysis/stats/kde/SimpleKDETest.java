package macrobase.analysis.stats.kde;

import macrobase.analysis.stats.kde.kernel.GaussianKernel;
import macrobase.analysis.stats.kde.kernel.Kernel;
import macrobase.util.data.TinyDataSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SimpleKDETest {
    protected List<double[]> tinyData(int n) {
        List<double[]> data = new ArrayList<>();
        for (int i = 0; i < n; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            data.add(sample);
        }
        return data;
    }


    @Test
    public void oneDTest() {
        SimpleKDE kde = new SimpleKDE();

        List<double[]> data = tinyData(5);

        double[] bw = new double[1];
        bw[0] = 2;
        kde.setBandwidth(bw);
        kde.train(data);
        assertEquals(2.0, kde.getBandwidth()[0], 1e-10);
        assertEquals(2.1400523, -Math.log(kde.density(data.get(0))), 1e-5);
        assertEquals(1.9142246, -Math.log(kde.density(data.get(1))), 1e-5);
    }

    @Test
    public void threeDTest() throws Exception {
        List<double[]> data = new TinyDataSource().get();
        SimpleKDE kde = new SimpleKDE()
                .setBandwidth(new double[]{2, 2, 2});
        kde.train(data);
        assertEquals(6.64640837, -Math.log(kde.density(data.get(0))), 1e-7);
        assertEquals(7.20467317, -Math.log(kde.density(data.get(3))), 1e-7);
    }

    @Test
    public void testIgnoreSelf() throws Exception {
        List<double[]> data = tinyData(2);
        double[] bw = {1};
        Kernel k = new GaussianKernel().initialize(bw);
        SimpleKDE kde = new SimpleKDE()
                .setBandwidth(bw)
                .setIgnoreSelf(true)
                .train(data);
        assertEquals(k.density(bw), kde.density(bw), 1e-10);
    }

}
