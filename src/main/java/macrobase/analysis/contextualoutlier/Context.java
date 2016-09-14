package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.util.BitSetUtil;
import macrobase.util.HypothesisTesting;

public class Context {
    private static final Logger log = LoggerFactory.getLogger(Context.class);
    
    private List<Datum> data; //the full database
    /**
     * A list of ordered contextual dimensions this node represents
     * and intervals for each dimension
     */
    private List<Integer> dimensions = new ArrayList<Integer>();
    private List<Interval> intervals = new ArrayList<Interval>();
    private int size = -1;
    private List<Context> parents = new ArrayList<Context>();
    private HashSet<Context> oneDimensionalAncestors = new HashSet<Context>();
    //the outlier detector used for detection in this context
    private BatchTrainScore detector;
    //the following is for context pruning
  
    private MacroBaseConf conf;
    
    //The outliers found in this context
    private BitSet outlierBitSet = null;
    private BitSet ancestorOutlierBitSet = null;
    private int numberOfOutliersNotContainedInAncestor = 0;

    //The actual data in this context
    private BitSet bitSet;
    
    public BitSet getBitSet() {
        return bitSet;
    }

    /**
     * This is called by one-dimensional context
     * @param bitSet
     */
    public void setBitSet(BitSet bitSet) {
        this.bitSet = bitSet;
        setSize(bitSet.cardinality()); 
    }
    
    /**
     * This is called by global context, and more than 1-dimensional context
     * @param data
     */
    public void setBitSet(List<Datum> data) {
        //global context
        if (parents.size() == 0) {
            BitSet bs = new BitSet(data.size());
            bs.set(0, data.size());
            setBitSet(bs);
        }
        
        //context whose parents are known
        if (parents.size() >= 2) {
            Context p1 = parents.get(0);
            Context p2 = parents.get(1);
            //both paraents must have already been set
            BitSet b1 = p1.getBitSet();
            BitSet b2 = p2.getBitSet();
            BitSet bs = null;
            bs = (BitSet) b1.clone();
            bs.and(b2);
            setBitSet(bs);
        }
    }

    public void freeBitSet() {
        bitSet = null;
    }
    
    /**
     * Global context
     */
    public Context(List<Datum> data, MacroBaseConf conf) {
        this.data = data;
        this.conf = conf;
        setBitSet(data);
    }

    /**
     * Initialize a one dimensional context
     *
     * @param dimension
     * @param interval
     * @param parent
     */
    public Context(int dimension, Interval interval, Context parent) {
        this.data = parent.data;
        dimensions.add(dimension);
        intervals.add(interval);
        parents.add(parent);
        
        oneDimensionalAncestors.add(this);
        
        this.conf = parent.conf;
        
        setAncestorOutlierBitSet();
    }

    public Context(List<Integer> dimensions, List<Interval> intervals, List<Context> parentContexts) {
        //parentContexts.size() >= 2
        Context parent1 = parentContexts.get(0);
        Context parent2 = parentContexts.get(1);
        
        this.data = parent1.data;
        this.dimensions = dimensions;
        this.intervals = intervals;
        
        parents.addAll(parentContexts);
        
        oneDimensionalAncestors.addAll(parent1.oneDimensionalAncestors);
        oneDimensionalAncestors.addAll(parent2.oneDimensionalAncestors);
        this.conf = parent1.conf;
        
        setAncestorOutlierBitSet();
        
           
    }

    /**
     * Determine if the unit contains the tuple
     *
     * @param datum
     * @return
     */
    public boolean containDatum(Datum datum) {
        int discreteDimensions = datum.getContextualDiscreteAttributes().size();
        int doubleDimensions = datum.getContextualDoubleAttributes().getDimension();
        int totalDimensions = discreteDimensions + doubleDimensions;
        for (int i = 0; i < dimensions.size(); i++) {
            int k = dimensions.get(i);
            if (k >= 0 && k < discreteDimensions) {
                int value = datum.getContextualDiscreteAttributes().get(k);
                if (!intervals.get(i).contains(value))
                    return false;
            } else if (k >= discreteDimensions && k < totalDimensions) {
                double value = datum.getContextualDoubleAttributes().getEntry(k - discreteDimensions);
                if (!intervals.get(i).contains(value))
                    return false;
            }
        }
        return true;
    }

    /**
     * Join this Context with other Context, can only be joined if the first (k-1) dimensions
     * have the same interval, and the last dimension has different interval
     * <p>
     * can be joined only if the size of the context is at least minSize
     *
     * @param other
     * @return
     */
    public Context join(Context other, List<Datum> data, double tau, ContextIndexTree cit) {
        //create new dimensions and intervals
        List<Integer> newDimensions = new ArrayList<Integer>();
        List<Interval> newIntervals = new ArrayList<Interval>();
        List<Integer> dimensions1 = dimensions;
        List<Integer> dimensions2 = other.dimensions;
        if (dimensions1.size() != dimensions2.size())
            return null;
        for (int i = 0; i < dimensions1.size(); i++) {
            int dimension1 = dimensions1.get(i);
            int dimension2 = dimensions2.get(i);
            Interval interval1 = intervals.get(i);
            Interval interval2 = other.intervals.get(i);
            if (i != dimensions1.size() - 1) {
                if (dimension1 != dimension2)
                    return null;
                if (interval1 != interval2)
                    return null;
                newDimensions.add(dimension1);
                newIntervals.add(interval1);
            } else {
                newDimensions.add(dimension1);
                newIntervals.add(interval1);
                newDimensions.add(dimension2);
                newIntervals.add(interval2);
            }
        }
        
        List<Context> parentContexts = new ArrayList<Context>();
        if (checkJoinConditions(newIntervals, cit, parentContexts) == false) {
            return null;
        }
        
        //create new context
        Context newUnit = new Context(newDimensions, newIntervals, parentContexts);
        
        //get the actual data, and check if it can be pruned
        if (newUnit.getBitSet() == null) {
            newUnit.setBitSet(data);
        }
        //check the actual density
        double realDensity = (double) newUnit.getBitSet().cardinality() / data.size();
        if (realDensity < tau) {
            ContextStats.numDensityPruningsUsingAll++;
            return null;
        }
        
        //dependency pruning
        boolean trivialityPrunedUsingAll = trivialityPruningUsingAllData(newUnit);        
        if (trivialityPrunedUsingAll) {
            ContextStats.numTrivialityPruning++;
            return null;
        }
        
        setDataDrivenPrunedFromParents(newUnit);
       
        return newUnit;
    }

    private boolean checkJoinConditions(List<Interval> intervals, ContextIndexTree cit, List<Context> parentContexts) {
        for (int i = 0; i < intervals.size(); i++) {
            //skip 
            List<Interval> skippedIntervals = new ArrayList<Interval>(intervals);
            skippedIntervals.remove(i);
            Context parentContext = cit.getContext(skippedIntervals);
            if (parentContext == null) {
                return false;
            }
            parentContexts.add(parentContext);
        }
        return true;
    }
  
    
    private boolean trivialityPruningUsingAllData(Context c) {
        boolean trivialityPruning = (conf != null )?conf.getBoolean(MacroBaseConf.CONTEXTUAL_PRUNING_TRIVIALITY, MacroBaseDefaults.CONTEXTUAL_PRUNING_TRIVIALITY):false;
        if (trivialityPruning == false)
            return false;
        for (Context p: c.getParents()) {
            BitSet b = p.getBitSet();
            double covered = BitSetUtil.subSetCoverage(b, c.getBitSet());
            if (covered >= 1.0) {
                return true;
            } 
        }
        return false;
    }
    
    public String print(DatumEncoder encoder) {
        if (dimensions.size() == 0) {
            return "Global Context: ";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dimensions.size(); i++) {
            sb.append(intervals.get(i).print(encoder) + " ");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        if (dimensions.size() == 0) {
            return "Global Context: ";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dimensions.size(); i++) {
            sb.append(intervals.get(i).toString() + " ");
        }
        return sb.toString();
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    
    public List<Interval> getIntervals() {
        return intervals;
    }

    public List<Integer> getDimensions() {
        return dimensions;
    }
    
    public List<Context> getParents() {
        return parents;
    }
    
    public HashSet<Context> getOneDimensionalAncestors() {
        return oneDimensionalAncestors;
    }

    public BatchTrainScore getDetector() {
        return detector;
    }

    public void setDetector(BatchTrainScore detector) {
        this.detector = detector;
    }

    public BitSet getOutlierBitSet() {
        return outlierBitSet;
    }

    public void setOutlierBitSet(BitSet outlierBitSet) {
        this.outlierBitSet = outlierBitSet;
    }
    
    public BitSet getAncestorOutlierBitSet() {
        return ancestorOutlierBitSet;
    }

    public void setAncestorOutlierBitSet() {
        ancestorOutlierBitSet = new BitSet(data.size());
        for (Context p: getParents()) {
            BitSet pOutlierBS = p.getOutlierBitSet();
            if (pOutlierBS != null) {
                ancestorOutlierBitSet.or(pOutlierBS);
            }
        }
    }
    
    public int getNumberOfOutliersNotContainedInAncestor() {
        return numberOfOutliersNotContainedInAncestor;
    }

    public void setNumberOfOutliersNotContainedInAncestor(int numberOfOutliersNotContainedInAncestor) {
        this.numberOfOutliersNotContainedInAncestor = numberOfOutliersNotContainedInAncestor;
    }
    
    private double median;
    private double MAD;
    public double getMedian() {
        return median;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    public double getMAD() {
        return MAD;
    }

    public void setMAD(double mAD) {
        MAD = mAD;
    }

    private boolean[] dataDrivenPruned;
    public void setDataDrivenPruned(boolean[] dataDrivenPruned) {
        this.dataDrivenPruned = dataDrivenPruned;
    }
    public void setDataDrivenPruned(int c) {
        dataDrivenPruned[c] = true;
    }
    public boolean[] getDataDrivenPruned() {
        return dataDrivenPruned;
    }
    public boolean getDataDrivenPruned(int c) {
        return dataDrivenPruned[c];
    }
    
    private void setDataDrivenPrunedFromParents(Context c) {
        if (c.getParents().get(0).dataDrivenPruned != null) {
            c.dataDrivenPruned = new boolean[c.getParents().get(0).dataDrivenPruned.length];
            for (int i = 0; i < c.dataDrivenPruned.length; i++) 
                c.dataDrivenPruned[i] = false;
            for (Context p: c.getParents()) {
                for (int i = 0; i < c.dataDrivenPruned.length; i++)  {
                    if (p.dataDrivenPruned[i] == true) {
                        c.dataDrivenPruned[i] = true;
                    }
                }
            }
        } 
    }
    
    //this context can be pruned if all clusters say it can be pruned
    public boolean getDataDrivenPrunedAll() {
        for (boolean d: dataDrivenPruned) {
            if (d == false)
                return false;
        }
        return true;
    }
    
}
