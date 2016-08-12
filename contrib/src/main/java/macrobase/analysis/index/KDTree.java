package macrobase.analysis.index;

import macrobase.datamodel.Datum;
import macrobase.datamodel.DatumComparator;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import java.util.Collections;
import java.util.List;

public class KDTree {

    private int leafCapacity;
    protected KDTree loChild;
    protected KDTree hiChild;
    protected int k;
    protected List<Datum> items;
    // Statistics
    protected int nBelow;
    protected RealVector mean;
    private int splitDimension;
    private double splitValue;
    // Array of (k,2) dimensions, of (min, max) pairs in all k dimensions
    private double[][] boundaries;

    /**
     * Build a KD-Tree that makes the splits based on the midpoint of the widest dimension.
     * This is the approach described in [Gray, Moore 2003] based on [Deng, Moore 1995].
     * @param data
     * @param leafCapacity
     */
    public KDTree(List<Datum> data, int leafCapacity) {
        this.leafCapacity = leafCapacity;
        this.k = data.get(0).metrics().getDimension();
        this.boundaries = new double[k][2];

        boundaries = AlgebraUtils.getBoundingBox(data);

        if (data.size() > this.leafCapacity) {

            double[] differences = new double[this.k];
            for (int i = 0; i < k; i++) {
                differences[i] = this.boundaries[i][1] - this.boundaries[i][0];
            }

            int widestDimension = 0;
            double maxDidth = -1;
            for (int i = 0; i < k ; i++) {
                if (differences[i] > maxDidth) {
                    maxDidth = differences[i];
                    widestDimension = i;
                }
            }

            this.splitDimension = widestDimension;

            // XXX: This is the slow part!!!
            Collections.sort(data, new DatumComparator(splitDimension));

            int splitIndex = data.size() / 2;
            Datum belowSplit = data.get(splitIndex - 1);
            Datum aboveSplit = data.get(splitIndex);
            this.splitValue = 0.5 * (
                    aboveSplit.metrics().getEntry(splitDimension) + belowSplit.metrics().getEntry(splitDimension)
            );

            this.loChild = new KDTree(data.subList(0, splitIndex), leafCapacity);
            this.hiChild = new KDTree(data.subList(splitIndex, data.size()), leafCapacity);
            this.nBelow = data.size();

            this.mean = (loChild.mean.mapMultiply(loChild.nBelow)
                         .add(hiChild.mean.mapMultiply(hiChild.nBelow))
                         .mapDivide(loChild.nBelow + hiChild.nBelow));
        } else {
            this.items = data;
            this.nBelow = data.size();

            RealMatrix ret = new Array2DRowRealMatrix(data.size(), this.k);
            RealVector sum = new ArrayRealVector(this.k);

            int index = 0;
            for (Datum d : data) {
                ret.setRow(index, d.metrics().toArray());
                sum = sum.add(d.metrics());
                index += 1;
            }

            this.mean = sum.mapDivide(this.nBelow);
        }
    }


    /**
     * Estimates min and max difference absolute vectors from point to region
     * @param queryDatum target point
     * @return minVec, maxVec
     */
    // TODO: Make this method faster.
    public RealVector[] getMinMaxDistanceVectors(Datum queryDatum) {
        double[] minDifferences = new double[k];
        double[] maxDifferences = new double[k];

        RealVector metrics = queryDatum.metrics();
        for (int i=0; i<k; i++) {
            double deltaLo = metrics.getEntry(i) - this.boundaries[i][0];
            double deltaHi = this.boundaries[i][1] - metrics.getEntry(i);
            // point is outside
            double minD = Math.abs(deltaLo);
            double maxD = Math.abs(deltaHi);
            if (minD < maxD) {
                minDifferences[i] = minD;
                maxDifferences[i] = maxD;
            } else {
                minDifferences[i] = maxD;
                maxDifferences[i] = minD;
            }

            if (deltaLo > 0 && deltaHi > 0) {
                // Point is inside so only add to max distance.
                minDifferences[i] = 0;
            }
        }

        RealVector[] rtn = new RealVector[2];
        rtn[0] = new ArrayRealVector(minDifferences);
        rtn[1] = new ArrayRealVector(maxDifferences);
        return rtn;
    }

    /**
     * Estimates bounds on the distance to a region
     * @param queryDatum target point
     * @return array with min, max distances squared
     */
    public double[] estimateL2DistanceSquared(Datum queryDatum) {
        RealVector vector = queryDatum.metrics();
        double[] estimates = new double[2];
        for (int i=0; i<k; i++) {
            double deltaLo = vector.getEntry(i) - this.boundaries[i][0];
            double deltaHi = this.boundaries[i][1] - vector.getEntry(i);
            double sqDeltaLo = deltaLo * deltaLo;
            double sqDeltaHi = deltaHi * deltaHi;

            // point is outside
            if (deltaLo < 0 || deltaHi < 0) {
                // Add the bigger distance to the longer estimate;
                if (sqDeltaHi < sqDeltaLo) {
                    estimates[0] += sqDeltaHi;
                    estimates[1] += sqDeltaLo;
                } else {
                    estimates[0] += sqDeltaLo;
                    estimates[1] += sqDeltaHi;
                }
            } else {
                // Point is inside so only add to max distance.
                // The point is inside the tree boundaries.
                estimates[1] += Math.max(sqDeltaHi, sqDeltaLo);
            }
        }
        return estimates;
    }

    public boolean isInsideBoundaries(Datum queryDatum) {
        RealVector vector = queryDatum.metrics();
        for (int i=0; i<k; i++) {
            if (vector.getEntry(i) < this.boundaries[i][0] || vector.getEntry(i) > this.boundaries[i][1]) {
                return false;
            }
        }
        return true;
    }

    public List<Datum> getItems() {
        return this.items;
    }

    public RealVector getMean() {
        return this.mean;
    }

    public double[][] getBoundaries() {
        return this.boundaries;
    }

    public KDTree getLoChild() {
        return this.loChild;
    }

    public KDTree getHiChild() {
        return this.hiChild;
    }

    public boolean isLeaf() {
        return this.loChild == null && this.hiChild == null;
    }

    public int getnBelow() {
        return nBelow;
    }

    public int getSplitDimension() {
        return splitDimension;
    }

    public String toString(int indent) {
        int nextIndent = indent + 1;
        String tabs = new String(new char[nextIndent]).replace("\0", "\t");
        if (loChild != null && hiChild != null) {
            return String.format("<KDNode: spitDim=%d splitVal=%.3f \n%sLO: %s\n%sHI: %s", this.splitDimension, this.splitValue, tabs, this.loChild.toString(nextIndent), tabs, this.hiChild.toString(nextIndent));
        }
        else if (hiChild!= null) {
            return String.format("<KDNode: splitDim=%d splitVal=%.3f \n%sHI: %s", this.splitDimension, this.splitValue, tabs, this.hiChild.toString(nextIndent));
        }
        else if (loChild != null) {
            return String.format("<KDNode: splitDim=%d splitVal=%.3f \n%sLO: %s", this.splitDimension, this.splitValue, tabs, this.loChild.toString(nextIndent));
        }
        else {
            String all = "<KDNode>:\n";
            for (Datum datum: this.items) {
                all += String.format("%s - %s\n", tabs, datum.metrics());
            }
            return all;
        }

    }

    public String toString() {
        return this.toString(0);
    }
}
