package msolver;

import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.CommonOps_DDRM;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class PointMassSolver {
    private int k;
    private int k2;

    private double[] mus;
    private double[] mmatVals;
    private DMatrixRMaj mmat;

    private PointMass[] points;
    private boolean isDiscrete;
    private int numPoints;

    private boolean verbose = false;

    public PointMassSolver(int k) {
        this.k = k;
        this.k2 = (int)((k+1)/2);
        this.mmatVals = new double[k2 * k2];
        this.mmat = DMatrixRMaj.wrap(k2, k2, mmatVals);
    }

    public class PointMass implements Comparable<PointMass> {
        public double loc;
        public double weight;
        public PointMass(double l, double w) {
            this.loc = l;
            this.weight = w;
        }

        @Override
        public int compareTo(PointMass o) {
            return Double.compare(loc, o.loc);
        }

        @Override
        public String toString() {
            return "PointMass{" +
                    "loc=" + loc +
                    ", weight=" + weight +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PointMass pointMass = (PointMass) o;
            return Double.compare(pointMass.loc, loc) == 0 &&
                    Double.compare(pointMass.weight, weight) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(loc, weight);
        }
    }

    public void setVerbose(boolean flag) {
        this.verbose = flag;
    }

    public void solve(double[] musArg) {
        this.mus = musArg.clone();
        double scaleFactor = mus[0];
        for (int i = 0; i < mus.length; i++) {
            mus[i] /= scaleFactor;
        }

        for (int i = 0; i < k2; i++) {
            System.arraycopy(mus, i, mmatVals, k2 *i, k2);
        }
        isDiscrete = false;
        numPoints = k2 -1;
        double curDeterminant = 1.0;
        for (int i = 1; i < k2; i++) {
            DMatrixRMaj subMat = CommonOps_DDRM.extract(mmat, 0, i, 0, i);
            double newDetermininant = CommonOps_DDRM.det(subMat);
            double ratio = newDetermininant / curDeterminant;
            curDeterminant = newDetermininant;
            if (verbose) {
                System.out.println(String.format("SubMatrix %d Ratio: %g", i, ratio));
            }
            if (Double.isNaN(ratio) || ratio < 1e-11) {
                numPoints = i-1;
                isDiscrete = true;
                break;
            }
        }

        double[] sMoments = new double[1];
        if (isDiscrete) {
            sMoments = Arrays.copyOf(mus, 2*numPoints+1);
        } else {
            sMoments = mus;
        }

        SimpleBoundSolver sSolver = new SimpleBoundSolver(sMoments.length);
        if (isDiscrete) {
            if (numPoints > 1) {
                SimpleBoundSolver.CanonicalDistribution dist = sSolver.getCanonicalDistribution(
                        sMoments
                );
                int n = dist.positions.length;
                points = new PointMass[n];
                for (int i = 0; i < n; i++) {
                    points[i] = new PointMass(dist.positions[i], dist.weights[i]);
                }
            } else {
                points = new PointMass[1];
                points[0] = new PointMass(mus[1], 1.0);
            }
        } else {
            double[] extremes = {-1, 1};
            SimpleBoundSolver.CanonicalDistribution[] dists = sSolver.getCanonicalDistributions(
                    sMoments, extremes
            );
            List<SimpleBoundSolver.CanonicalDistribution> toCombine = Arrays.asList(
//                    dist,
                    dists[0],
                    dists[1]
            );
            int nt = 0;
            for (SimpleBoundSolver.CanonicalDistribution d : toCombine) {
                nt += d.positions.length;
            }
            points = new PointMass[nt];

            int pointIdx = 0;
            for (SimpleBoundSolver.CanonicalDistribution d : toCombine) {
                for (int i = 0; i < d.positions.length; i++){
                    points[pointIdx] = new PointMass(
                            d.positions[i],
                            d.weights[i]/toCombine.size()
                    );
                    pointIdx++;
                }
            }
        }
        Arrays.sort(points);
    }

    public double getCDF(double x) {
        if (isDiscrete) {
            double totalWeight = 0;
            if (x < points[0].loc) {
                return totalWeight;
            }
            for (int i = 0; i < points.length; i++) {
                PointMass pm = points[i];
                if (pm.loc > x) {
                    return totalWeight;
                }
                totalWeight += pm.weight;
            }
            return 1.0;
        } else {
            int n = points.length;
            double cur_weight = 0;
            if (x < points[0].loc) {
                return cur_weight;
            }
            for (int i = 0; i < points.length-1; i++) {
                PointMass m1 = points[i];
                PointMass m2 = points[i+1];
                double bin_weight = (m1.weight + m2.weight)/2;
                if (i == 0) {
                    bin_weight += m1.weight/2;
                }
                if (i == points.length-2) {
                    bin_weight += m2.weight/2;
                }

                if (m2.loc > x) {
                    double f1 = (x - m1.loc) / (m2.loc - m1.loc);
                    return cur_weight + f1*bin_weight;
                }
                cur_weight += bin_weight;
            }
            return 1.0;
        }
    }

    public double getQuantile(double p) {
        if (isDiscrete) {
            double curWeight = 0;
            for (int i = 0; i < points.length; i++) {
                PointMass pm = points[i];
                curWeight += pm.weight;
                if (curWeight >= p) {
                    return pm.loc;
                }
            }
            return points[points.length-1].loc;
        } else {
            int n = points.length;
            double cur_weight = 0;
            double q = points[points.length-1].loc;
            for (int i = 0; i < points.length-1; i++) {
                PointMass m1 = points[i];
                PointMass m2 = points[i+1];
                double bin_weight = (m1.weight + m2.weight)/2;
                if (i == 0) {
                    bin_weight += m1.weight/2;
                }
                if (i == points.length-2) {
                    bin_weight += m2.weight/2;
                }

                double new_weight = cur_weight + bin_weight;
                if (new_weight >= p) {
                    double f1 = (p - cur_weight) / bin_weight;
                    q = m1.loc + f1*(m2.loc - m1.loc);
                    break;
                }

                cur_weight = new_weight;
            }
            return q;
        }
    }

    public boolean isDiscrete() {
        return isDiscrete;
    }
    public int getNumPoints() {
        return numPoints;
    }

    public PointMass[] getPoints() {
        return points;
    }
}
