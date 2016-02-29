package macrobase.analysis.outlier;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public class CovarianceMatrixAndMean {
    private RealMatrix covarianceMatrix;
    private RealVector mean;

    public CovarianceMatrixAndMean(RealVector mean, RealMatrix covarianceMatrix) {
        this.mean = mean;
        this.covarianceMatrix = covarianceMatrix;
    }

    public RealVector getMean() {
        return mean;
    }
    public void setMean(RealVector mean) {
        this.mean = mean;
    }
    public RealMatrix getCovarianceMatrix() {
        return covarianceMatrix;
    }
    public void setCovarianceMatrix(RealMatrix covarianceMatrix) {
        this.covarianceMatrix = covarianceMatrix;
    }
}
