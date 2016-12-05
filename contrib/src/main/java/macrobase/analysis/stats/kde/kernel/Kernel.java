package macrobase.analysis.stats.kde.kernel;

public abstract class Kernel {
    protected int dim;
    // store the inverse of the bandwidth for speed
    protected double[] invBandwidth;

    // Cache these constant multipliers
    protected double dimFactor;
    protected double bwFactor;

    public Kernel initialize(double[] bs) {
        this.dim = bs.length;
        this.invBandwidth = new double[dim];
        this.dimFactor = getDimFactor(dim);

        this.bwFactor = 1;
        for (int i = 0; i < dim; i++) {
            invBandwidth[i] = 1.0/bs[i];
            bwFactor *= invBandwidth[i];
        }
        return this;
    }

    public abstract double getDimFactor(int curDim);
    public abstract double density(double[] d);
}
