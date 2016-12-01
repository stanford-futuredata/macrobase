package macrobase.analysis.stats;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

import java.util.List;

public class Covariance {
    public static RealMatrix getCovariance(List<Datum> data) {
        int rank = data.get(0).metrics().getDimension();

        RealMatrix ret = new Array2DRowRealMatrix(data.size(), rank);
        int index = 0;
        for (Datum d : data) {
            ret.setRow(index, d.metrics().toArray());
            index += 1;
        }

        return (new org.apache.commons.math3.stat.correlation.Covariance(ret)).getCovarianceMatrix();
    }
}
