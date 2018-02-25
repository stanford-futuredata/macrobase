package msolver;

import msolver.chebyshev.ChebyshevPolynomial;
import org.apache.commons.math3.analysis.UnivariateFunction;

class GFunction implements UnivariateFunction {
    private boolean useStandardBasis;
    private double aCenter, aScale, bCenter, bScale;
    private ChebyshevPolynomial cBasis;

    public GFunction(
            int k, boolean useStandardBasis,
            double aCenter, double aScale,
            double bCenter, double bScale
    ) {
        this.cBasis = ChebyshevPolynomial.basis(k);
        this.useStandardBasis = useStandardBasis;
        this.aCenter = aCenter;
        this.aScale = aScale;
        this.bCenter = bCenter;
        this.bScale = bScale;
    }

    @Override
    public double value(double y) {
        double x = y * aScale + aCenter;
        double gX;
        if (useStandardBasis) {
            gX = Math.log(x);
        } else {
            gX = Math.exp(x);
        }
        double scaledBGX = (gX - bCenter) / bScale;
        return cBasis.value(scaledBGX);
    }
}
