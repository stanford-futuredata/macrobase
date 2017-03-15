package macrobase.analysis.stats.kde;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KDTree {
    // Parameters
    private int leafCapacity = 20;
    private boolean splitByWidth = false;

    // Core Data
    private int k;
    private KDTree loChild;
    private KDTree hiChild;
    private ArrayList<double[]> leafItems;
    private boolean trained = false;

    // Tracking element locations
    public int[] idxs;
    public int startIdx, endIdx;

    // Calculated Statistics
    private int splitDimension;
    private int nBelow;
    private double splitValue;
    // Array of (k,2) dimensions, of (min, max) pairs in all k dimensions
    private double[][] boundaries;

    public KDTree() {
        splitDimension = 0;
    }

    public KDTree(KDTree parent, boolean loChild) {
        this.k = parent.k;
        this.idxs = parent.idxs;
        this.splitDimension = (parent.splitDimension + 1) % k;
        this.boundaries = new double[k][2];
        for (int i=0;i<k;i++) {
            this.boundaries[i][0] = parent.boundaries[i][0];
            this.boundaries[i][1] = parent.boundaries[i][1];
        }
        if (loChild) {
            this.boundaries[parent.splitDimension][1] = parent.splitValue;
        } else {
            this.boundaries[parent.splitDimension][0] = parent.splitValue;
        }

        leafCapacity = parent.leafCapacity;
        splitByWidth = parent.splitByWidth;
    }

    public KDTree setSplitByWidth(boolean f) {
        this.splitByWidth = f;
        return this;
    }
    public KDTree setLeafCapacity(int leafCapacity) {
        this.leafCapacity = leafCapacity;
        return this;
    }

    public KDTree build(List<double[]> data) {
        int n = data.size();
        this.k = data.get(0).length;
        this.idxs = new int[n];
        this.splitDimension = 0;
        this.boundaries = AlgebraUtils.getBoundingBoxRaw(data);
        // Make a local copy of the data since we're going to sort it
        double[][] dataArray = new double[n][k];
        for (int i=0;i<n;i++) {
            dataArray[i] = data.get(i);
            this.idxs[i] = i;
        }
        buildRec(dataArray, 0, dataArray.length);
        return this;
    }

    private KDTree buildRec(double[][] data, int startIdx, int endIdx) {
        this.nBelow = endIdx - startIdx;
        this.startIdx = startIdx;
        this.endIdx = endIdx;

        if (endIdx - startIdx > this.leafCapacity) {
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            double[] splitValues = new double[endIdx-startIdx];
            for (int j=startIdx;j<endIdx;j++) {
                double curVal = data[j][splitDimension];
                splitValues[j-startIdx] = curVal;
                if (curVal < min) { min = curVal; }
                if (curVal > max) { max = curVal; }
            }
            boundaries[splitDimension][0] = min;
            boundaries[splitDimension][1] = max;

            Percentile p = new Percentile();
            p.setData(splitValues);
            if (splitByWidth) {
                this.splitValue = 0.5 * (p.evaluate(10) + p.evaluate(90));
            } else {
                this.splitValue = p.evaluate(50);
            }
            int l = startIdx;
            int r = endIdx - 1;
            while (true) {
                while ((l < endIdx) && data[l][splitDimension] < splitValue) {
                    l++;
                }
                while ((r >= startIdx) && data[r][splitDimension] >= splitValue) {
                    r--;
                }
                if (l < r) {
                    double[] tmp = data[l];
                    data[l]= data[r];
                    data[r]= tmp;

                    int tmpI = idxs[l];
                    this.idxs[l] = idxs[r];
                    this.idxs[r] = tmpI;
                } else {
                    break;
                }
            }
            if (l==startIdx || l==endIdx) {
                l = (startIdx + endIdx) / 2;
                this.splitValue = data[l][splitDimension];
            }
            this.loChild = new KDTree(this, true).buildRec(data, startIdx, l);
            this.hiChild = new KDTree(this, false).buildRec(data, l, endIdx);

        } else {
            this.leafItems = new ArrayList<>(leafCapacity);

            double[] sum = new double[k];
            for (int j=startIdx;j<endIdx;j++) {
                double[] d = data[j];
                leafItems.add(d);
            }
        }

        trained = true;
        return this;
    }

    /**
     * Estimates min and max difference absolute vectors from point to region
     * @return minVec, maxVec
     */
    public double[][] getMinMaxDistanceVectors(double[] q) {
        double[][] minMaxDiff = new double[2][k];

        for (int i=0; i<k; i++) {
            double d1 = q[i] - boundaries[i][0];
            double d2 = q[i] - boundaries[i][1];
            // outside to the right
            if (d2 >= 0) {
                minMaxDiff[0][i] = d2;
                minMaxDiff[1][i] = d1;
            }
            // inside, min distance is 0;
            else if (d1 >= 0) {
                minMaxDiff[1][i] = d1 > -d2 ? d1 : -d2;
            }
            // outside to the left
            else {
                minMaxDiff[0][i] = -d1;
                minMaxDiff[1][i] = -d2;
            }
        }

        return minMaxDiff;
    }

    /**
     * Estimates bounds on the distance to a region
     * @return array with min, max distances squared
     */
    public double[] getMinMaxDistances(double[] q) {
        double[][] diffVectors = getMinMaxDistanceVectors(q);
        double[] estimates = new double[2];
        for (int i = 0; i < k; i++) {
            double minD = diffVectors[0][i];
            double maxD = diffVectors[1][i];
            estimates[0] += minD * minD;
            estimates[1] += maxD * maxD;
        }
        return estimates;
    }

    public boolean isInsideBoundaries(double[] q) {
        for (int i=0; i<k; i++) {
            if (q[i] < this.boundaries[i][0] || q[i] > this.boundaries[i][1]) {
                return false;
            }
        }
        return true;
    }

    public ArrayList<double[]> getItems() {
        return this.leafItems;
    }

    public int getNBelow() {
        return nBelow;
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

    public KDTree[] getChildren(double[] d) {
        KDTree[] c = new KDTree[2];
        if (d[splitDimension] < splitValue) {
            c[0] = this.loChild;
            c[1] = this.hiChild;
        } else {
            c[0] = this.hiChild;
            c[1] = this.loChild;
        }
        return c;
    }

    public boolean isLeaf() {
        return this.loChild == null && this.hiChild == null;
    }

    public int getSplitDimension() {
        return splitDimension;
    }

    public double getSplitValue() {
        return splitValue;
    }

    public Map<String, Object> toMap(boolean verbose) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (loChild == null && hiChild == null) {
            if (verbose) {
                out.put("items", this.leafItems);
            } else {
                out.put("n", this.leafItems.size());
            }
        } else {
            out.put("dim", this.splitDimension);
            out.put("val", this.splitValue);
            if (loChild != null) {
                out.put("lo", loChild.toMap(verbose));
            }
            if (hiChild != null) {
                out.put("hi", hiChild.toMap(verbose));
            }
        }
        if (verbose) {
            out.put("sIdx", this.startIdx);
            out.put("eIdx", this.endIdx);
        }
        return out;
    }

    public String toString() {
        if (!trained) {
            return "Untrained Tree";
        } else {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(toMap(false));
        }
    }

    public String rawString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(toMap(true));
    }
}
