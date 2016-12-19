package macrobase.analysis.stats.kde.grid;

import macrobase.analysis.stats.kde.kernel.Kernel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CompositeGrid {
    private static final Logger log = LoggerFactory.getLogger(CompositeGrid.class);
    public Kernel kernel;
    public double[] baseGridSize;
    public List<Double> gridRatios;
    public double cutoff;
    public boolean ignoreSelf = false;

    public int numTrainPoints;
    public SoftGridCutoff[] grids;

    public CompositeGrid(
            Kernel k,
            double[] baseGridSize,
            List<Double> gridRatios,
            double cutoff
    ) {
        this.kernel = k;
        this.baseGridSize = baseGridSize;
        this.gridRatios = gridRatios;
        this.cutoff = cutoff;

        this.grids = new SoftGridCutoff[gridRatios.size()];
    }
    public CompositeGrid setIgnoreSelf(boolean f) {
        ignoreSelf = f;
        return this;
    }

    public CompositeGrid train(List<double[]> data) {
        numTrainPoints = data.size();
        double rawGridCutoff = data.size() * cutoff;
        for (int i = 0; i < grids.length; i++) {
            double[] gridSize = baseGridSize;
            double gridScaleFactor = gridRatios.get(i);
            for (int k = 0; k < gridSize.length; k++) {
                gridSize[k] *= gridScaleFactor;
            }
            grids[i] = new SoftGridCutoff(kernel, gridSize, rawGridCutoff);
        }
        for (SoftGridCutoff g : grids) {
            for (double[] d : data) {
                g.add(d);
            }
            g.prune();
        }
        return this;
    }

    public double density(double[] d) {
        for (SoftGridCutoff g : grids) {
            double curDensity = g.getUnscaledDenseValue(d);
            if (curDensity > 0.0) {
                if (ignoreSelf) {
                    return curDensity / (numTrainPoints - 1);
                } else {
                    return curDensity / numTrainPoints;
                }
            }
        }
        return 0.0;
    }
}
