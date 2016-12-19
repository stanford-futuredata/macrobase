package macrobase.analysis.stats.kde;

import macrobase.analysis.stats.kde.kernel.BandwidthSelector;
import macrobase.analysis.stats.kde.kernel.GaussianKernel;
import macrobase.analysis.stats.kde.kernel.Kernel;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class TreeKDE {
    private static final Logger log = LoggerFactory.getLogger(TreeKDE.class);

    // ** Basic stats parameters
    private int numPoints;
    private double[] bandwidth;
    private Kernel kernel;
    // If we reuse the training as the scoring set, whether to ignore the weight provided by query point
    private boolean ignoreSelf=false;

    // ** Tree parameters
    private KDTree tree;
    // Whether the user provided a pre-populated tree
    private boolean trainedTree = false;
    // Total density error tolerance
    private double tolerance = 0;
    // Upper and lower cutoff points to define region of interest
    private double cutoffH = Double.MAX_VALUE;
    private double cutoffL = 0.0;

    // ** Diagnostic Measurements
    public long finalCutoff[] = new long[10];
    public long numNodesProcessed[] = new long[10];
    public int numScored = 0;
    public int numKernels = 0;

    // ** Cached values
    private double unscaledTolerance;
    private double unscaledCutoffH;
    private double unscaledCutoffL;
    private double selfPointDensity;

    public TreeKDE(KDTree tree) {
        this.tree = tree;
    }

    public TreeKDE setIgnoreSelf(boolean b) {this.ignoreSelf = b; return this;}
    public TreeKDE setTolerance(double t) {this.tolerance = t; return this;}
    public TreeKDE setCutoffH(double cutoff) {this.cutoffH = cutoff; return this;}
    public TreeKDE setCutoffL(double cutoff) {this.cutoffL = cutoff; return this;}
    public TreeKDE setBandwidth(double[] bw) {this.bandwidth = bw; return this;}
    public TreeKDE setKernel(Kernel k) {this.kernel = k; return this;}
    public TreeKDE setTrainedTree(KDTree tree) {this.tree=tree; this.trainedTree=true; return this;}

    public double getCutoffH() {return this.cutoffH;}
    public double getCutoffL() {return this.cutoffL;}
    public double getTolerance() {return this.tolerance;}
    public KDTree getTree() {return this.tree;}

    public double[] getBandwidth() {return bandwidth;}

    public TreeKDE train(List<double[]> data) {
        if (data.isEmpty()) {
            throw new RuntimeException("Empty Training Data");
        }
        this.numPoints = data.size();
        this.unscaledTolerance = tolerance * numPoints;
        this.unscaledCutoffH = cutoffH * numPoints;
        this.unscaledCutoffL = cutoffL * numPoints;

        if (bandwidth == null) {
            bandwidth = new BandwidthSelector().findBandwidth(data);
        }
        if (kernel == null) {
            kernel = new GaussianKernel();
            kernel.initialize(bandwidth);
        }
        this.selfPointDensity = kernel.density(new double[bandwidth.length]);

        if (!trainedTree) {
            StopWatch sw = new StopWatch();
            sw.start();
            this.tree.build(data);
            sw.stop();
            log.debug("built kd-tree on {} points in {}", data.size(), sw.toString());
        }
        return this;
    }

    public static Comparator<ScoreEstimate> scoreEstimateComparator = (o1, o2) -> {
        if (o1.totalWMax < o2.totalWMax) {
            return 1;
        } else if (o1.totalWMax > o2.totalWMax) {
            return -1;
        } else {
            return 0;
        }
    };

    /**
     * Calculates density * N
     * @param d query point
     * @return unnormalized density
     */
    private double pqScore(double[] d) {
        // Initialize Score Estimates
        double totalWMin = 0.0;
        double totalWMax = 0.0;
        long curNodesProcessed = 0;
        if (ignoreSelf) {
            totalWMin -= selfPointDensity;
            totalWMax -= selfPointDensity;
        }

        PriorityQueue<ScoreEstimate> pq = new PriorityQueue<>(100, scoreEstimateComparator);
        ScoreEstimate initialEstimate = new ScoreEstimate(this.kernel, this.tree, d);
        numKernels += 2;
        pq.add(initialEstimate);
        totalWMin += initialEstimate.totalWMin;
        totalWMax += initialEstimate.totalWMax;
        curNodesProcessed++;

//        System.out.println("\nScoring : "+Arrays.toString(d));
//        System.out.println("tolerance: "+unscaledTolerance);
//        System.out.println("cutoff: "+unscaledCutoff);
        boolean useMinAsFinalScore = false;
        while (!pq.isEmpty()) {
//            System.out.println("minmax: "+totalWMin+", "+totalWMax);
            if (totalWMax - totalWMin < unscaledTolerance) {
                numNodesProcessed[0] += curNodesProcessed;
                finalCutoff[0]++;
                break;
            } else if (totalWMin > unscaledCutoffH) {
                numNodesProcessed[1] += curNodesProcessed;
                finalCutoff[1]++;
                useMinAsFinalScore = true;
                break;
            }
            else if (totalWMax < unscaledCutoffL) {
                numNodesProcessed[2] += curNodesProcessed;
                finalCutoff[2]++;
                break;
            }
            ScoreEstimate curEstimate = pq.poll();
//            System.out.println("current box:\n"+ AlgebraUtils.array2dToString(curEstimate.tree.getBoundaries()));
//            System.out.println("split: "+curEstimate.tree.getSplitDimension() + ":"+curEstimate.tree.getSplitValue());
            totalWMin -= curEstimate.totalWMin;
            totalWMax -= curEstimate.totalWMax;

            if (curEstimate.tree.isLeaf()) {
                double exact = exactDensity(curEstimate.tree, d);
                numKernels += curEstimate.tree.getNBelow();
                totalWMin += exact;
                totalWMax += exact;
            } else {
                ScoreEstimate[] children = curEstimate.split(this.kernel, d);
                numKernels += children.length * 2;
                curNodesProcessed += 2;
                for (ScoreEstimate child : children) {
                    totalWMin += child.totalWMin;
                    totalWMax += child.totalWMax;
                    pq.add(child);
                }
            }
        }
        if (pq.isEmpty()) {
            finalCutoff[3]++;
        }
        numScored++;
        if (useMinAsFinalScore) {
            // totalWMax can be completely inaccurate if we stop based on cutoff
            return totalWMin;
        } else {
            return (totalWMin + totalWMax) / 2;
        }
    }

    private double exactDensity(KDTree t, double[] d) {
        double score = 0.0;
        for (double[] dChild : t.getItems()) {
            double[] diff = d.clone();
            for (int i = 0; i < diff.length; i++) {
                diff[i] -= dChild[i];
            }
            double delta = kernel.density(diff);
            score += delta;
        }
        return score;

    }

    public void showDiagnostics() {
        log.info("Final Loop Cutoff: tol {}, > cutoff {}, < cutoff {}, completion {}",
                finalCutoff[0],
                finalCutoff[1],
                finalCutoff[2],
                finalCutoff[3]
                );
        log.info("Avg # of nodes processed: tol {}, > cutoff {}, < cutoff {}, completion {}",
                (double)numNodesProcessed[0]/finalCutoff[0],
                (double)numNodesProcessed[1]/finalCutoff[1],
                (double)numNodesProcessed[2]/finalCutoff[2],
                (double)numNodesProcessed[3]/finalCutoff[3]
                );
    }

    /**
     * Returns normalized pdf
     */
    public double density(double[] d) {
        if (ignoreSelf) {
            return pqScore(d) / (numPoints-1);
        } else {
            return pqScore(d) / numPoints;
        }
    }

    public int getNumKernels() {
        return numKernels;
    }
}
