package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;

import macrobase.analysis.stats.KDE;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

public class Discretization {

    private static final Logger log = LoggerFactory.getLogger(Discretization.class);

    private List<Datum> inputData;
    private double[] values;
    
    public Discretization() {
        
    }
    
    public Discretization(double[] values) {
        this.values = values;
    }
    
    public Discretization(List<Datum> inputData) {
       this.inputData = inputData;
       
       values = new double[inputData.size()];
       for (int i = 0; i < inputData.size(); i++) {
           values[i] = inputData.get(i).getMetrics().getEntry(0);
       }
 
    }
    
    public List<Interval> clusterOneDimensionalValues(MacroBaseConf conf) throws ConfigurationException {
        
        MacroBaseConf.CONTEXTUAL_DATADRIVEN_CLUSTERING_ALGORITHM clusteringAlgo = conf.getContextualDataDrivenClusteringAlgorithm();
        log.debug("Clustering Algorithm: {}", clusteringAlgo);
        if (clusteringAlgo == MacroBaseConf.CONTEXTUAL_DATADRIVEN_CLUSTERING_ALGORITHM.EQUI_WIDTH ) {
            int numIntervals = conf.getInt(MacroBaseConf.CONTEXTUAL_NUMINTERVALS,
                    MacroBaseDefaults.CONTEXTUAL_NUMINTERVALS);
            return equiWidth(numIntervals);
        }
        if (clusteringAlgo == MacroBaseConf.CONTEXTUAL_DATADRIVEN_CLUSTERING_ALGORITHM.DBSCAN ) {
            double epsilon = conf.getDouble(MacroBaseConf.CONTEXTUAL_DATADRIVEN_DBSCAN_EPSILON, MacroBaseDefaults.CONTEXTUAL_DATADRIVEN_DBSCAN_EPSILON);
            double denseContextTau = conf.getDouble(MacroBaseConf.CONTEXTUAL_DENSECONTEXTTAU, MacroBaseDefaults.CONTEXTUAL_DENSECONTEXTTAU);
            int minClusterSize = new Double(inputData.size() * denseContextTau).intValue();
            return dbScan(epsilon, minClusterSize);
        } else if (clusteringAlgo == MacroBaseConf.CONTEXTUAL_DATADRIVEN_CLUSTERING_ALGORITHM.KDE) {
            int numIntervals = conf.getInt(MacroBaseConf.CONTEXTUAL_NUMINTERVALS,
                    MacroBaseDefaults.CONTEXTUAL_NUMINTERVALS);
            return denseRegionsUsingKDE(numIntervals);
        } else if (clusteringAlgo == MacroBaseConf.CONTEXTUAL_DATADRIVEN_CLUSTERING_ALGORITHM.KMEANS) {
            int numClusters = conf.getInt(MacroBaseConf.CONTEXTUAL_DATADRIVEN_KMEANS_K, MacroBaseDefaults.CONTEXTUAL_DATADRIVEN_KMEANS_K);
            return kMeans(numClusters);
        } else {
            return null;
        }
    }
    
    public List<Interval> agglomerativeHierarchical(int numClusters) {
        return null;
    }
    
    public List<Interval> kMeans(int numClusters) {
        List<Interval> result = new ArrayList<Interval>();

        KMeansPlusPlusClusterer<DoublePoint> kMeans = new KMeansPlusPlusClusterer<DoublePoint>(numClusters,1000);
        List<DoublePoint> points = new ArrayList<DoublePoint>();
        for (double value: values) {
            double[] point = new double[1];
            point[0] = value;
            points.add(new DoublePoint(point));
        }
        
        List<CentroidCluster<DoublePoint>> clusterResults = kMeans.cluster(points);
        // output the clusters
        for (int i=0; i<clusterResults.size(); i++) {
            /*
            System.out.println("Cluster " + i);
            for (DoublePoint point : clusterResults.get(i).getPoints())
                System.out.println(point.getPoint()[0]);
            System.out.println();
            */
            double minValue = Double.MAX_VALUE;
            double maxValue = Double.MIN_VALUE;
            for (DoublePoint point: clusterResults.get(i).getPoints()) {
                double value = point.getPoint()[0];
                if (value > maxValue) 
                    maxValue = value;
                if (value < minValue) 
                    minValue = value;
            }
            Interval interval = new IntervalDouble(minValue, maxValue + 0.0000001);
            result.add(interval);
        }
              
        printIntervals(result);
        return result;
    }
    
    
    public List<Interval> dbScan(double epsilon, int minPoints) {
        List<Interval> result = new ArrayList<Interval>();
        
        DBSCANClusterer<DoublePoint> dbScan = new DBSCANClusterer<DoublePoint>(epsilon, minPoints);
        
        List<DoublePoint> points = new ArrayList<DoublePoint>();
        for (double value: values) {
            double[] point = new double[1];
            point[0] = value;
            points.add(new DoublePoint(point));
        }
        
        List<Cluster<DoublePoint>> clusters = dbScan.cluster(points);
        for (Cluster<DoublePoint> cluster: clusters) {
            //dbscan did a dedup of points
            System.out.println("Cluster " + cluster.getPoints());
            
            double minValue = Double.MAX_VALUE;
            double maxValue = Double.MIN_VALUE;
            for (DoublePoint point: cluster.getPoints()) {
                double value = point.getPoint()[0];
                if (value > maxValue) 
                    maxValue = value;
                if (value < minValue) 
                    minValue = value;
            }
            Interval interval = new IntervalDouble(minValue, maxValue + 0.0000001);
            result.add(interval);
        }
        
        printIntervals(result);
        return result;
    }
    
    
    public List<Interval> denseRegionsUsingKDE(int numIntervals) {
        List<Interval> allIntervals = new ArrayList<Interval>();
        
        MacroBaseConf kdeConf = new MacroBaseConf()
                .set(MacroBaseConf.KDE_KERNEL_TYPE, KDE.KernelType.EPANECHNIKOV_MULTIPLICATIVE)
                .set(MacroBaseConf.KDE_BANDWIDTH_ALGORITHM, KDE.BandwidthAlgorithm.NORMAL_SCALE);
        
        //make a copy
        List<Datum> data = new ArrayList<Datum>(inputData);
        //sort the data based on its metric value
        Collections.sort(data, new Comparator<Datum>(){
            @Override
            public int compare(Datum o1, Datum o2) {
                if (o1.getMetrics().getEntry(0) > o2.getMetrics().getEntry(0)) {
                    return 1;
                } else if (o1.getMetrics().getEntry(0) < o2.getMetrics().getEntry(0)){
                    return -1;
                } else {
                    return 0;
                }
            }
            
        });
        
        KDE kde = new KDE(kdeConf);
        kde.setProportionOfDataToUse(1);
        kde.train(data);
        
        double[] scores = new double[data.size()];
        for (int i = 0; i < data.size(); i++) {
            scores[i] = kde.score(data.get(i));
        }
        
        int[] locals = new int[data.size()]; // 1 if its local maxima, -1 if local minima, 0 otherwise
        for (int i = 0; i < data.size(); i++) {
            if (i == 0) {
                double scoreCurrent = scores[i];
                double scoreNext = scores[i+1];
                //the first point can only be a maxima
                if (scoreNext <= scoreCurrent) {
                    locals[i] = 1;
                } else {
                    locals[i] = 0;
                }
            } else if (i == data.size() - 1) {
                //the last point can only be a maxima
                double scorePrev = scores[i - 1];
                double scoreCurrent = scores[i] ;
                if (scoreCurrent > scorePrev) {
                    locals[i] = 1;
                } else {
                    locals[i] = 0;
                }
            } else {
                double scorePrev = scores[i - 1];
                double scoreCurrent = scores[i];
                double scoreNext = scores[i +1];
                if (scoreCurrent > scorePrev && scoreNext <= scoreCurrent) {
                    locals[i] = 1;
                } else if (scoreCurrent < scorePrev && scoreNext >= scoreCurrent) {
                    locals[i] = -1;
                } else {
                    locals[i] = 0;
                }
            }
        }
        
        double leftClosed = Double.MIN_VALUE;
        double rightOpen = Double.MAX_VALUE; 
        
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        for (int i = 0; i < data.size(); i++) {
            double value = data.get(i).getMetrics().getEntry(0);
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
        }
        
        leftClosed = min - 1.0;
        rightOpen = max + 1.0;
        
        for (int i = 0; i < data.size(); i++) {
            if (locals[i] == -1) {
                rightOpen = data.get(i).getMetrics().getEntry(0);
                Interval interval = new IntervalDouble(leftClosed, rightOpen);
                allIntervals.add(interval);
                
                leftClosed = data.get(i).getMetrics().getEntry(0);
            }
            
            if (i == data.size() - 1) {
                rightOpen = max + 1.0;
                Interval interval = new IntervalDouble(leftClosed, rightOpen);
                allIntervals.add(interval);
            }
        }
       
        
        System.err.println("Before merging.....");

        for (Interval interval: allIntervals) {
            int count = 0;
            for (Datum d: inputData) {
                if (interval.contains(d.getMetrics().getEntry(0))) {
                    count++;
                }
            }
            System.err.println(interval.toString() + "\t" + count);
        }
        
        System.err.println("After merging.....");

        double minCount = 100;
        if (minCount >= inputData.size()) 
            return allIntervals;
        List<Interval> mergedIntervals = mergeIntervals(allIntervals, minCount);
        printIntervals(mergedIntervals);
      
        return mergedIntervals;
    }
    
    public void printIntervals(List<Interval> mergedIntervals) {
        log.debug("Print Discretization Results:");

        for (Interval interval: mergedIntervals) {
            int count = 0;
            for (double value: values) {
                if (interval.contains(value)) {
                    count++;
                }
            }
            /*
            for (Datum d: inputData) {
                if (interval.contains(d.getMetrics().getEntry(0))) {
                    count++;
                }
            }*/
            log.debug(interval.toString() + "\t" + count);
        }
    }
    
    private List<Interval> mergeIntervals(List<Interval> intervals, double minCount) {
        List<Interval> merged = new ArrayList<Interval>();
        
        Map<Interval, Integer> interval2Count = new HashMap<Interval, Integer>();
        Map<Interval, Boolean> interval2Merged = new HashMap<Interval, Boolean>();
        for (Interval interval: intervals) {
            interval2Merged.put(interval, false);
            int count = 0;
            for (Datum d: inputData) {
                if (interval.contains(d.getMetrics().getEntry(0))) {
                    count++;
                }
            }
            interval2Count.put(interval, count);
        }
        
        //pick the interval with the largest count as the center for merging!
        while(true) {
            //get the largest unmerged interval
            Interval largestCountInterval = null;
            int largestIntervalIndex = -1;
            int maxCount = -1;
            for (int i = 0; i < intervals.size(); i++) {
                if (interval2Merged.get(intervals.get(i)))
                    continue;
                int count = interval2Count.get(intervals.get(i));
                if (count > maxCount) {
                    largestCountInterval = intervals.get(i);
                    largestIntervalIndex = i;
                    maxCount = count;
                }
            }
            if (largestCountInterval == null) {
                //all intervals have been merged
                break;
            }
            
            if (interval2Count.get(largestCountInterval) >= minCount) {
                //already large enough
                interval2Merged.put(largestCountInterval, true);
                merged.add(largestCountInterval);
            } else {
                //merge left and right at the same time until it reaches the minimum 
                interval2Merged.put(largestCountInterval, true);
                int currentCount = maxCount;
                double mergedMin = ((IntervalDouble)largestCountInterval).getMin();
                double mergedMax = ((IntervalDouble)largestCountInterval).getMax();
                boolean hasMoreLeft = true;
                boolean hasMoreRight = true;
                
                int more = 1;
                while(true) {
                    if (hasMoreLeft) {
                        hasMoreLeft = (largestIntervalIndex - more >= 0) && (interval2Merged.get(intervals.get(largestIntervalIndex - more)) == false);
                    }
                    if (hasMoreRight) {
                       hasMoreRight = (largestIntervalIndex + more < intervals.size()) && (interval2Merged.get(intervals.get(largestIntervalIndex + more)) == false);
                    }
                    
                    if (hasMoreLeft && hasMoreRight) {
                        int leftCount = interval2Count.get(intervals.get(largestIntervalIndex - more));
                        int rightCount = interval2Count.get(intervals.get(largestIntervalIndex + more));
                        
                        mergedMin = ((IntervalDouble)intervals.get(largestIntervalIndex - more)).getMin();
                        mergedMax = ((IntervalDouble)intervals.get(largestIntervalIndex + more)).getMax();
                        interval2Merged.put(intervals.get(largestIntervalIndex - more), true);
                        interval2Merged.put(intervals.get(largestIntervalIndex + more), true);

                        currentCount += leftCount + rightCount;
                        if (currentCount  >= minCount) {
                           Interval mergedInterval = new IntervalDouble(mergedMin, mergedMax);
                           merged.add(mergedInterval);
                           break;
                        }
                    } else if(hasMoreLeft && !hasMoreRight) {
                        int leftCount = interval2Count.get(intervals.get(largestIntervalIndex - more));
                        mergedMin = ((IntervalDouble)intervals.get(largestIntervalIndex - more)).getMin();
                        interval2Merged.put(intervals.get(largestIntervalIndex - more), true);
                        
                        currentCount += leftCount;
                        if (currentCount >= minCount) {
                            Interval mergedInterval = new IntervalDouble(mergedMin, mergedMax);
                            merged.add(mergedInterval);
                            break;
                        }

                    } else if (!hasMoreLeft && hasMoreRight) {
                        int rightCount = interval2Count.get(intervals.get(largestIntervalIndex + more));
                        mergedMax = ((IntervalDouble)intervals.get(largestIntervalIndex + more)).getMax();
                        interval2Merged.put(intervals.get(largestIntervalIndex + more), true);

                        currentCount += rightCount;
                        if (currentCount >= minCount) {
                            Interval mergedInterval = new IntervalDouble(mergedMin, mergedMax);
                            merged.add(mergedInterval);
                            break;
                        }

                    } else {
                        Interval mergedInterval = new IntervalDouble(mergedMin, mergedMax);
                        merged.add(mergedInterval);
                        break;
                    }
                    more++;
                }
                
            }
        }
        
        //get rid of under supported interval
        Collections.sort(merged, new Comparator<Interval>(){

            @Override
            public int compare(Interval o1, Interval o2) {
               if (((IntervalDouble)o1).getMin() > ((IntervalDouble)o2).getMin()) {
                   return 1;
               } else  if (((IntervalDouble)o1).getMin() < ((IntervalDouble)o2).getMin()) {
                   return -1;
               } else {
                   return 0;
               }
            }
            
        });
        Map<Interval, Integer> interval2Support = new HashMap<Interval, Integer>();
        for (Interval interval: merged) {
            int count = 0;
            for (Datum d: inputData) {
                if (interval.contains(d.getMetrics().getEntry(0))) {
                    count++;
                }
            }
            interval2Support.put(interval, count);
            //System.err.println(interval.toString() + "\t" + count);
        }
        
        
        while(true) {
            Interval underSupport = null;
            int underSupportIndex = -1;
            for (int i = 0; i < merged.size(); i++) {
                if (interval2Support.get(merged.get(i)) < minCount) {
                    underSupport = merged.get(i);
                    underSupportIndex = i;
                    break;
                }
            }
            if (underSupport == null)
                break;
            
            int leftSize = (underSupportIndex - 1 >=0)? interval2Support.get(merged.get(underSupportIndex - 1)): -1;
            int rightSize = (underSupportIndex + 1 < merged.size())? interval2Support.get(merged.get(underSupportIndex + 1)): -1;
            if (leftSize != -1 && rightSize != -1) {
                if (leftSize < rightSize) {
                    //merge with left
                    Interval newInterval = new IntervalDouble(((IntervalDouble)merged.get(underSupportIndex-1)).getMin(), ((IntervalDouble)underSupport).getMax());
                    merged.set(underSupportIndex - 1, newInterval);
                    merged.remove(underSupportIndex);
                    interval2Support.put(newInterval, leftSize + interval2Support.get(underSupport));
                } else {
                    //merge with right
                    Interval newInterval = new IntervalDouble( ((IntervalDouble)underSupport).getMin(), ((IntervalDouble)merged.get(underSupportIndex+1)).getMax());
                    merged.set(underSupportIndex + 1, newInterval);
                    merged.remove(underSupportIndex);
                    interval2Support.put(newInterval, rightSize + interval2Support.get(underSupport));

                }
            } else if (leftSize != -1) {
                //merge with left
                Interval newInterval = new IntervalDouble(((IntervalDouble)merged.get(underSupportIndex-1)).getMin(), ((IntervalDouble)underSupport).getMax());
                merged.set(underSupportIndex - 1, newInterval);
                merged.remove(underSupportIndex);
                interval2Support.put(newInterval, leftSize + interval2Support.get(underSupport));
            } else if (rightSize != -1) {
                //merge with right
                Interval newInterval = new IntervalDouble( ((IntervalDouble)underSupport).getMin(), ((IntervalDouble)merged.get(underSupportIndex+1)).getMax());
                merged.set(underSupportIndex + 1, newInterval);
                merged.remove(underSupportIndex);
                interval2Support.put(newInterval, rightSize + interval2Support.get(underSupport));
            } else {
                
            }
           
        }
       
        
        return merged;
    }
    
   
    public List<Interval> equiWidth(int numIntervals) {
        List<Interval> allIntervals = new ArrayList<Interval>();
        //equi-width
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        //find out the min, max
        for (Double value: values) {
            if (value > max) {
                max = value;
            }
            if (value < min) {
                min = value;
            }
        }
        // divide the interval into numIntervals
        double step = (max - min) / numIntervals;
        double start = min;
        for (int i = 0; i < numIntervals; i++) {
            if (i != numIntervals - 1) {
                Interval interval = new IntervalDouble(start, start + step);
                start += step;
                allIntervals.add(interval);
            } else {
                //make the max a little bit larger
                Interval interval = new IntervalDouble(start, max + 0.000001);
                allIntervals.add(interval);
            }
        }
        
        printIntervals(allIntervals);
        return allIntervals;
    }
    
   
    
    /**
     * This only works if there is not a lot of duplciates in vlaues
     * @param numIntervals
     * @return
     */
    public List<Interval> equiDepth(int numIntervals) {
        List<Interval> allIntervals = new ArrayList<Interval>();
        Double[] copyed = new Double[values.length];
        for (int i = 0; i < values.length; i++) 
            copyed[i] = values[i];
        Arrays.sort(copyed);
        
        int[] intervelLength = new int[numIntervals];
        int temp1 = values.length / numIntervals;
        int temp2 = values.length % numIntervals;
        for (int i = 0; i < numIntervals; i++) {
            if (i < temp2) 
                intervelLength[i] = temp1 + 1;
            else
                intervelLength[i] = temp1;
        }
        
         
        int previousStart = 0;
        for (int i = 0; i < numIntervals; i++) {
            //the start and end index of i'th interval
            int start = previousStart;
            int end = start + intervelLength[i];
            
            double startValue = copyed[start];
            double endValue = copyed[end - 1];
            //did not handle cases, where endValue and start value are the same
            Interval interval = new IntervalDouble(startValue, endValue + 0.00000001);
            allIntervals.add(interval);
            previousStart = end;
        }
        
        printIntervals(allIntervals);
        return allIntervals;
    }
    
 
    /**
     * This only works if there is not a lot of duplciates in vlaues
     * @param numIntervals
     * @return
     */
    public List<Interval> splittingByMedian(int numIntervals) {
        
        Arrays.sort(values);
        
        List<Interval> all = splittingByMedianHelper(null);
        while(all.size() < numIntervals) {
            List<Interval> newAll = new ArrayList<Interval>();
            for (Interval interval: all) {
                newAll.addAll(splittingByMedianHelper( interval));
            }
            all = newAll;
        }
        return all;
    }
    
    private List<Interval> splittingByMedianHelper(Interval interval) {
        
        List<Double> valuesList = new ArrayList<Double>();
        for (int i = 0; i < values.length; i++) {
            if (interval == null) {
                valuesList.add(values[i]);
            } else  if (interval.contains(values[i])) {
                valuesList.add(values[i]);
            }
        }
        
        
        List<Interval> allIntervals = new ArrayList<Interval>();
        
        if (valuesList.size() == 0) {
            return allIntervals;
        }
        
        double medianValue = 0;  
        if (valuesList.size() % 2 == 1) {
            medianValue = valuesList.get(valuesList.size() / 2);
        } else {
            medianValue = (valuesList.get(valuesList.size() / 2) + valuesList.get(valuesList.size() / 2 - 1) ) * 0.5;
        }
        
        Interval smallerInterval = new IntervalDouble(valuesList.get(0), medianValue);
        Interval biggerInterval = new IntervalDouble(medianValue, valuesList.get(valuesList.size()-1) + 0.0001);
        
        allIntervals.add(smallerInterval);
        allIntervals.add(biggerInterval);
        
        return allIntervals;
    }
    
    
    

    public static void main(String[] args) {
        double[] valuesTest = new double[]{1,1,2,2,3,3,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100};
        Discretization dTest = new Discretization(valuesTest);
        //dTest.dbScan(1.0, 2);
        dTest.kMeans(5);
    }

}
