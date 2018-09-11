package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


/**
 * dataxray: single machine version
 */
public class DataXRaySolver {
    /**
     *  define structure vector class
     *  example: feature vector (ALL:ALL:ALL) with structure vector (0:0:0)
     */
    public class StructureVector extends ArrayList<Integer>{
        int level = 0; // accumulated levels
        private static final long serialVersionUID = 1L;

        /** Initialize */
        public StructureVector(){
            super();
        }

        /** Initialize from string */
        public StructureVector(String line){
            // initialize feature vector from a line
            String[] dimensions = line.split(":");
            for(String dimension : dimensions) {
                this.add(Integer.valueOf(dimension));
                level += Integer.valueOf(dimension);
            }
        }

        /** convert to string */
        public String toString(){
            StringBuilder str = new StringBuilder();
            for(int level : this)
                str.append(String.valueOf(level) + ":");
            return str.toString();
        }
    }

    /**
     *  define feature vector class
     */
    public class FeatureVector extends ArrayList<String>{

        private static final long serialVersionUID = 1L;

        /** Initialize */
        public FeatureVector(){
            super();
        }

        /** Initialize from string */
        public FeatureVector(String line){
            // initialize feature vector from a line
            String[] dimensions = line.split(":");
            for(String dimension : dimensions)
                this.add(dimension);
        }

        /** convert to string */
        public String toString(){
            StringBuilder str = new StringBuilder();
            for(String feature : this)
                str.append(feature + ":");
            return str.toString();
        }
    }
    /**
     * define Element class
     * */
    public class Element {

        private int id; // unique element id
        private boolean value; // feature value: true of false
        private int dims; // number of dimensions for this element
        private ArrayList<ArrayList<String>> featureInfo; // feature information

        /** initialize element */
        public Element(){
            this.value = true;
            this.featureInfo = new ArrayList<ArrayList<String>>();
        }

        /** initialize element from string */
        public Element(String line){
            // 1%true%all,l0f1,l1f2...:all,l0f4,l1f5,...:,...:...
            String[] pieces = line.split("%");
            id = Integer.valueOf(pieces[0]); // initialize ID
            value  = Boolean.valueOf(pieces[1]); // initialize value
            featureInfo = new ArrayList<ArrayList<String>>(); // initialize feature info

            // update feature info
            String[] dimensions = pieces[2].split(":");
            for(int i = 0; i < dimensions.length; ++i){
                ArrayList<String> dim = new ArrayList<String>();
                String[] levels = dimensions[i].split("_");
                for(String level : levels)
                    dim.add(level);
                featureInfo.add(dim);
            }
            dims = featureInfo.size();
        }

        /** covert to string */
        public String toString(){
            String result = id + "%" + String.valueOf(value) + "%";
            for(ArrayList<String> featureOneDim : featureInfo){
                for(String featureStr : featureOneDim){
                    result += featureStr + "_";
                }
                result += ":";
            }
            return result;
        }

        /** get feature vector given a structure vector*/
        public FeatureVector getFeatureVector(StructureVector structureVector){

            // initialize feature vector
            FeatureVector featureVector = new FeatureVector();
            // update feature vector
            for(int i = 0; i < structureVector.size(); ++i){
                ArrayList<String> currentFDim = featureInfo.get(i); // get feature names in given dimension
                featureVector.add(currentFDim.get(structureVector.get(i))); // update feature dimension
            }
            return featureVector;
        }
    }
    /**
     * define feature class
     */
    public class Feature {

        private double alpha; // parameter in cost function
        private FeatureVector featureVector; // full feature vector
        private StructureVector structureVector; // full structure vector

        private double errorrate; // error rate
        private double cost; // cost
        public int[] maxDimLevel; // maxDimLevel information
        private int maxLevel; // maximum accumulate level through all feature dimensions
        private HashMap<Integer, Element> listOfElement; // list of elements
        private boolean flagS; // true: ancestor feature(s) selected

        /** initialize feature */
        public Feature(){
            errorrate = -1;
            cost = -1;
            listOfElement = new HashMap<Integer, Element>();
            flagS = false;
            structureVector = new StructureVector();
            featureVector = new FeatureVector();
        }
        /** Initialize feature by a string */
        public Feature(String line){
            String[] properties = line.split(";"); // split properties
            // update max level
            String[] maxs = properties[0].split(":");
            maxDimLevel = new int[maxs.length];
            for(int i = 0; i < maxs.length; ++i)
                maxDimLevel[i] = Integer.valueOf(maxs[i]);

            errorrate = Double.valueOf(properties[1]); // update error rate
            cost = Double.valueOf(properties[2]); // update cost
            flagS = Boolean.valueOf(properties[3]); // update is_select flag

            featureVector = new FeatureVector(properties[4]); // update feature vector
            structureVector = new StructureVector(properties[5]); // update structure vector

            // update elements
            listOfElement = new HashMap<Integer, Element>();
            String[] elementstr = properties[8].split("=");
            for(int i = 0; i < elementstr.length; ++i){
                Element element = new Element(elementstr[i]);
                listOfElement.put(element.id, element);
            }
        }

        /** Initialize feature by buffered reader */
        public Feature(BufferedReader br) throws IOException {
            final String firstLine = br.readLine();
            final String[] properties = firstLine.split("\t")[1].split(";"); // split properties
            // update max level
            String[] maxs = properties[0].split(":");
            maxDimLevel = new int[maxs.length];
            for(int i = 0; i < maxs.length; ++i)
                maxDimLevel[i] = Integer.valueOf(maxs[i]);

            errorrate = Double.valueOf(properties[1]); // update error rate
            cost = Double.valueOf(properties[2]); // update cost
            flagS = Boolean.valueOf(properties[3]); // update is_select flag

            featureVector = new FeatureVector(properties[4]); // update feature vector
            structureVector = new StructureVector(properties[5]); // update structure vector

            // update elements
            listOfElement = new HashMap<>();
            String line;
            while ((line = br.readLine()) != null) {
                Element element = new Element(line);
                listOfElement.put(element.id, element);
            }
        }

        /** initialize feature */
        public Feature(FeatureVector f_, StructureVector s_, double a_, int[] maxDimLevel_, ArrayList<Element> elements){
            featureVector = f_;
            structureVector = s_;
            alpha = a_;
            errorrate = -1;
            cost = -1;
            listOfElement = new HashMap<Integer, Element>();
            flagS = false;
            maxDimLevel = maxDimLevel_;
            maxLevel = 0;
            for(int level : maxDimLevel)
                maxLevel += level;
            for(Element element : elements)
                listOfElement.put(element.id, element);
        }

        /** assign maxDimLevel */
        public void assignmaxDimLevel(int[] maxDimLevel_){
            maxDimLevel = maxDimLevel_;
            maxLevel = 0;
            for(int level : maxDimLevel)
                maxLevel += level;
        }

        /** get current accumulated level */
        public int getLevel(){
            int levelsum = 0;
            for(int level : structureVector) {
                levelsum += level;
            }
            return levelsum;
        }

        /* add elements */
        public void addAllElement(ArrayList<Element> elements){
            for(Element element : elements)
                listOfElement.put(element.id, element);
        }

        /** add single element */
        public void addElement(Element element){
            listOfElement.put(element.id, element);
        }

        /** calculate error rate and cost */
        public void calculateEC(){

            if (dataXRayMap.containsKey(this.featureVector.toString())) {
                double[] tuple = dataXRayMap.get(this.featureVector.toString());
                this.errorrate = tuple[0];
                this.cost = tuple[1];
                return;
            }

            // get true element count and false element count
            int tcount = 0, fcount = 0;
            for(Element element : this.listOfElement.values()){
                if(element.value) {
                    tcount++;
                } else {
                    fcount++;
                }
            }

            // calculate error rate
            this.errorrate = (fcount+tcount) > 0 ? (double) (fcount)/(fcount+tcount) : 0;

            // adapt alpha
            // alpha = this.errorrate * this.listOfElement.size();
            // calculate cost

            if(errorrate > 0 && errorrate < 1)
                this.cost = Math.log(1/alpha)/Math.log(2) + fcount*Math.log(1/errorrate)/Math.log(2) + tcount*Math.log(1/(1-errorrate))/Math.log(2);
            else
                this.cost = this.errorrate == 0 ? 0 : Math.log(1/alpha)/Math.log(2);
        }

        /** get a list of parent feature vectors */
        public ArrayList<String> parentFeatureGenerator(){
            // initialize result
            ArrayList<String> parentList = new ArrayList<String>();

            // choose one element
            Element sampleelement = listOfElement.values().iterator().next();

            // get parent feature in all dimensions
            for(int i = 0; i < structureVector.size(); ++i){

                // must above the first level in current feature dimension;
                // otherwise, no parent feature in this dimension
                if(structureVector.get(i) > 0){

                    // update structure vector
                    structureVector.set(i, structureVector.get(i)-1);

                    // get feature vector
                    FeatureVector featureVector = sampleelement.getFeatureVector(structureVector);

                    String parent = String.valueOf(i) + " " + featureVector.toString();
                    // update result
                    parentList.add(parent);

                    // update structure vector
                    structureVector.set(i, structureVector.get(i) + 1);
                }
            }
            // return
            return parentList;
        }

        /** get a list of child features */
        public ArrayList<StructureVector> nextFeatureGenerator(){
            // initialize result
            ArrayList<StructureVector> childList = new ArrayList<StructureVector>();

            // get features
            for(int i = 0; i < structureVector.size(); ++i){
                if(structureVector.get(i) < maxDimLevel[i]){

                    // update structure vector
                    structureVector.set(i, structureVector.get(i)+1);

                    childList.add((StructureVector) structureVector.clone());

                    // update structure vector
                    structureVector.set(i, structureVector.get(i)-1);
                }
            }
            return childList;
        }
    }

    ArrayList<Element> allElements = new ArrayList<Element>(); // a  list of elements
    public ArrayList<Feature> badFeatures = new ArrayList<Feature>(); // solved result
    int dims;  // number of dimensions
    public int[] maxDimLevel; // maximum values for features
    double alpha; // fixed cost parameter: in cost function

    // other parameters
    double goodFeatureErrorBound = 0.9; // upper bound for error rate
    double badFeatureErrorBound = 0.6; // lower bound for error rate
    double goodFeatureVarianceBound = 0.1; // upper bound for variance
    double badFeatureVarianceBound = 0.05; // lower bound for variance
    Map<String, double[]> dataXRayMap;

    /**
     * dataxray solver initialization
     * @param alpha_ alpha in the cost function
     * @param gErrB_ early decision parameter: good feature error rate
     * @param bErrB_ early decision parameter: bad feature error rate
     * @param gVarB_ early decision parameter: good feature variance
     * @param bVarB_ early decision parameter: bad feature variance
     *  */
    public DataXRaySolver(
            double alpha_,
            double gErrB_,
            double bErrB_,
            double gVarB_,
            double bVarB_,
            Map<String, double[]> dataXRayMap){
        alpha = alpha_;
        this.goodFeatureErrorBound = gErrB_;
        this.badFeatureErrorBound = bErrB_;
        this.goodFeatureVarianceBound = gVarB_;
        this.badFeatureVarianceBound = bVarB_;
        this.dataXRayMap = dataXRayMap;
    }

    /** Save selected bad features in file
     *	@param filename outputfile name
     *  */
    public void printBadFeature(String filename){
        try{
            File file = new File(filename);
            if(!file.exists())
                file.createNewFile();
            FileWriter filewriter = new FileWriter(file.getAbsoluteFile());
            BufferedWriter out = new BufferedWriter(filewriter);
            for(Feature feature : badFeatures){
                out.write(feature.featureVector + "\t" + feature.structureVector + " ;" + feature.errorrate + " " + ";" + feature.cost + ";" + feature.listOfElement.size());
                out.newLine();
            }
            out.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * solve function
     * */
    public void solveFeatures() throws Exception{
        badFeatures.clear();
        // initial feature
        StructureVector rootSV = new StructureVector();
        FeatureVector rootFV = new FeatureVector();
        for(int dim = 0; dim < dims; ++dim){
            rootSV.add(0);
            rootFV.add("a");
        }
        // create initial feature with all elements
        Feature rootFeature = new Feature(rootFV, rootSV, alpha, maxDimLevel, allElements);
        rootFeature.calculateEC();

        // initialize current level features
        HashMap<String, Feature> currentLevel = new HashMap<String, Feature>();

        // add initial feature
        currentLevel.put(rootFeature.featureVector.toString(), rootFeature);
        boolean isContinue = true;
        while(isContinue){
            // for every feature in current level
            // features in next level
            HashMap<String, Feature> featureSet = new HashMap<String, Feature>(); // list of child features
            HashMap<String, Feature> S = new HashMap<String, Feature>();
            HashMap<String, Feature> U = new HashMap<String, Feature>();

            // for every parent feature
            for(Feature parentFeature : currentLevel.values()){
                // get structure vector list in next level
                ArrayList<StructureVector> childSVList = parentFeature.nextFeatureGenerator();

                // get features in next level
                for(Element element : parentFeature.listOfElement.values()){
                    for(int j = 0; j < childSVList.size(); ++j){
                        FeatureVector childFV = element.getFeatureVector(childSVList.get(j));
                        Feature childFeature;
                        if(featureSet.containsKey(childFV.toString())) {
                            childFeature = featureSet.get(childFV.toString());
                        } else {
                            childFeature = new Feature((FeatureVector) childFV.clone(), (StructureVector) childSVList.get(j).clone(), this.alpha, maxDimLevel, new ArrayList<Element>());
                            featureSet.put(childFV.toString(), childFeature);
                        }
                        // update flag in S info
                        childFeature.flagS = (childFeature.flagS || parentFeature.flagS);
                        // add element
                        childFeature.addElement(element);
                    }
                }
            }

            // initial feature set with features that are already selected
            HashMap<String, Feature> featuresInS = new HashMap<String, Feature>();
            for(Feature feature : featureSet.values()){
                // calculate details
                feature.calculateEC();
                // check flag in S, update featureInS.
                if(feature.flagS)
                    featuresInS.put(feature.featureVector.toString(), feature);
            }

            // get partitions: partitionidentifier + list of child features
            HashMap<String, ArrayList<Feature>> parentPartition = new HashMap<String, ArrayList<Feature>>();
            for(Feature childFeature : featureSet.values()){

                // get a set of unique parition# + parentfeaturevector identifiers
                ArrayList<String> parentFVList = childFeature.parentFeatureGenerator();

                for(int i = 0; i < parentFVList.size(); ++i){
                    ArrayList<Feature> childFeatureList;

                    if(parentPartition.containsKey(parentFVList.get(i))) {
                        childFeatureList = parentPartition.get(parentFVList.get(i));
                    } else {
                        childFeatureList = new ArrayList<Feature>();
                        parentPartition.put(parentFVList.get(i), childFeatureList);
                    }
                    childFeatureList.add(childFeature);
                }
            }

            // update S, U set in each partition, never consider feature with flatS = true
            for(String partitionID : parentPartition.keySet()){
                // get parent featurevector
                String[] info = partitionID.split(" ");
                String parentFV = info[1];
                if(currentLevel.containsKey(parentFV)){
                    // get parent feature
                    Feature parentFeature = currentLevel.get(parentFV);
                    if(parentFeature.flagS)
                        continue;
                    // get child features
                    ArrayList<Feature> childFeatureList = parentPartition.get(partitionID);
                    // calculate variance
                    double[] variancecost = this.getVarianceCost(childFeatureList);

                    boolean parentInS;
                    if(goodQuality(parentFeature, variancecost[0], childFeatureList.size())) {
                        // early accept
                        parentInS = true;
                    } else {
                        if(badQuality(parentFeature,variancecost[0])) {
                            // early prune
                            parentInS = false;
                        } else {
                            // compare cost
                            parentInS = (parentFeature.cost < variancecost[1]);
                        }
                    }
                    if(parentInS){
                        // add parent feature in S set, child features in U set
                        S.put(parentFeature.featureVector.toString(), parentFeature);
                        for(Feature childFeature : childFeatureList)
                            U.put(childFeature.featureVector.toString(), childFeature);
                    }
                    else{
                        // add parent feature in U set, child features in S set
                        for(Feature childFeature : childFeatureList)
                            S.put(childFeature.featureVector.toString(), childFeature);
                        U.put(parentFeature.featureVector.toString(), parentFeature);
                    }
                }
            }

            // combine S and U
            HashMap<String, Feature> nextLevel =  new HashMap<String, Feature>();
            isContinue = false;
            for(Feature feature : S.values()){
                // only accept feature if
                // 1. error rate > 0
                // 2. not in U
                // 3. flagS = false: ancestor(s) not selected
                if(feature.errorrate > 0 && !U.containsKey(feature.featureVector.toString()) && !feature.flagS){
                    // select the current feature as a good feature:
                    // if it is a parent feature, save to result list
                    // if it is a child feature, continue drilling down
                    if(currentLevel.containsKey(feature.featureVector.toString())){ // is a parent feature
                        // add into result set
                        badFeatures.add(feature);
                        // System.out.println(f.getFeatureVector().toString() + " " + f.getElements().size());
                        // update child feature flag in S into
                        for(int dim = 0; dim < dims; ++dim){
                            String partitionID = String.valueOf(dim) + " " + feature.featureVector.toString();
                            if(parentPartition.containsKey(partitionID)){
                                // get child features
                                ArrayList<Feature> childFeatureList = parentPartition.get(partitionID);
                                // update flagS for each child feature
                                for(Feature childFeature : childFeatureList){
                                    childFeature.flagS = true;
                                    nextLevel.put(childFeature.featureVector.toString(), childFeature);
                                }
                            }
                        }
                    }
                    else{
                        // add feature in final result list if it is the finest feature
                        if(feature.getLevel() == feature.maxLevel){
                            badFeatures.add(feature);
                        } else {
                            // add feature for further exploration
                            nextLevel.put(feature.featureVector.toString(), feature);
                            isContinue = true;
                        }
                    }
                }
            }
            // add features already selected before current level
            nextLevel.putAll(featuresInS);
            currentLevel = nextLevel;
        }
    }

    /**
     * compute total cost and variance given a set of features
     * @param features calculate cost and variance of given list of features
     * */
    public double[] getVarianceCost(ArrayList<Feature> features){
        // calculate mean value
        double avgErrRate = 0;
        double cost = 0;
        for(Feature feature : features){
            avgErrRate += feature.errorrate;
            cost += feature.cost;
        }
        avgErrRate = avgErrRate / features.size();
        // calculate variance
        double variance = 0;
        for(Feature feature: features){
            variance += Math.pow(feature.errorrate - avgErrRate, 2);
        }
        variance = variance/features.size();
        double[] values = new double[2];
        values[0] = variance;
        values[1] = cost;
        return values;
    }

    /** compute accumulate cost given a set of features */
    public double getCost(ArrayList<Feature> features){
        double cost = 0;
        for(Feature f:features){
            if(!f.flagS)
                cost += f.cost;
        }
        return cost;
    }

    /** return true if given feature is a good feature */
    public boolean goodQuality(Feature feature, double variance, int size){
        if(feature.errorrate > this.goodFeatureErrorBound && variance < this.goodFeatureVarianceBound && size > 1) {
            return true;
        } else {
            return false;
        }
    }

    /** return true if given feature is a bad feature */
    public boolean badQuality(Feature feature, double variance){
        if(feature.errorrate < this.badFeatureErrorBound || variance > this.badFeatureVarianceBound) {
            return true;
        } else {
            return false;
        }
    }

    public void readData(String inputpath) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(inputpath));
//        String line = br.readLine();
        Feature feature = new Feature(br);
        br.close();
        allElements = new ArrayList<Element>();
        allElements.addAll(feature.listOfElement.values());
        maxDimLevel = feature.maxDimLevel;
        dims = maxDimLevel.length;
    }
}

