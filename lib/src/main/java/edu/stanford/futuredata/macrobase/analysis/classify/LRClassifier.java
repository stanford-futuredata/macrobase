package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;

import java.lang.Math;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class LRClassifier implements Transformer {
	private List<String> columnNames;
	private String attributeName;
	private DataFrame output = null;
	public List<String> topFeatures;

	public LRClassifier(String attributeName, String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
        this.attributeName = attributeName;
        this.topFeatures = new ArrayList<String>();
    }

	@Override
    public void process(DataFrame input) {
        topFeatures = new ArrayList<String>();
    	List<double[]> columns = new ArrayList<double[]>();
    	String[] attributeColumn = input.getStringColumnByName(attributeName);
    	for (String columnName : columnNames) {
            double[] metrics = input.getDoubleColumnByName(columnName);
            columns.add(metrics);
        }
        Map<String, Integer> encoder = new HashMap<String, Integer>();
        int nextInt = 0;
        double[][] x = new double[input.getNumRows()][columns.size()];
        int[] y = new int[input.getNumRows()];
        for (int i = 0; i < input.getNumRows(); i++) {
        	double[] example = new double[columns.size()];
        	for (int j = 0; j < columns.size(); j++) {
        		example[j] = columns.get(j)[i];
        	}
        	x[i] = example;

        	if (encoder.containsKey(attributeColumn[i])) {
        		y[i] = encoder.get(attributeColumn[i]);
        	} else {
        		encoder.put(attributeColumn[i], nextInt);
        		y[i] = nextInt;
        		nextInt++;
        	}
        }
        double[][] W = new double[1][1];
        try {
            LogisticRegression lr = new LogisticRegression(x, y, 0.0001, 0.01, 100);
            W = lr.W;
        } catch(Exception e) {
            System.out.format("Error: %s\n", Arrays.toString(W));
            return;
        }
        for (int i = 0; i < encoder.size(); i++) {
        	double[] weights = W[i];
        	double top_weight = 0;
        	int top_feature = 0;
        	for (int j = 0; j < columns.size(); j++) {
        		if (weights[j] > top_weight) {
        			top_weight = weights[j];
        			top_feature = j;
        		}
        	}
        	topFeatures.add(columnNames.get(top_feature));
        }
    }

    @Override
    public DataFrame getResults() {
        return output;
    }
}