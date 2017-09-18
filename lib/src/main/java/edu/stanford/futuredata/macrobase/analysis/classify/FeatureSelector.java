package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import smile.feature.SumSquaresRatio;
import org.apache.commons.math3.stat.inference.OneWayAnova;

import java.lang.Math;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class FeatureSelector {
	private List<String> columnNames;
	private String attributeName;
	private DataFrame output = null;
	public List<String> topFeatures;

	public FeatureSelector(String attributeName, String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
        this.attributeName = attributeName;
        this.topFeatures = new ArrayList<String>();
    }

    public List<Double> process_anova(DataFrame input) {
    	List<double[]> columns = new ArrayList<double[]>();
    	String[] attributeColumn = input.getStringColumnByName(attributeName);
    	for (String columnName : columnNames) {
            double[] metrics = input.getDoubleColumnByName(columnName);
            columns.add(metrics);
        }
        Map<String, Integer> encoder = new HashMap<String, Integer>();
        Map<Integer, MutableInt> counts = new HashMap<Integer, MutableInt>();
        int nextInt = 0;
        int[] y = new int[input.getNumRows()];
        for (int i = 0; i < input.getNumRows(); i++) {
        	if (encoder.containsKey(attributeColumn[i])) {
        		y[i] = encoder.get(attributeColumn[i]);
                counts.get(y[i]).increment();
        	} else {
        		encoder.put(attributeColumn[i], nextInt);
        		y[i] = nextInt;
                counts.put(y[i], new MutableInt(1));
        		nextInt++;
        	}
        }

        List<Double> feature_ranks = new ArrayList<Double>();
        OneWayAnova anova = new OneWayAnova();
        for (int i = 0; i < columns.size(); i++) {
            double[] column = columns.get(i);
            List<double[]> category_data = new ArrayList<double[]>();
            int[] insertedSoFar = new int[encoder.size()];
            for (int j = 0; j < encoder.size(); j++) {
                category_data.add(new double[counts.get(j).get()]);
                insertedSoFar[j] = 0;
            }
            for (int j = 0; j < input.getNumRows(); j++) {
                int category = encoder.get(attributeColumn[j]);
                category_data.get(category)[insertedSoFar[category]] = column[j];
                insertedSoFar[category]++;
            }
            // for (int j = 0; j < encoder.size(); j++) {
            //     List<Double> category_list = data.get(j);
            //     double[] category_arr = new double[category_list.size()];
            //     for (int k = 0; k < category_arr.length; k++) {
            //         category_arr[k] = category_list.get(k);
            //     }
            //     category_data.add(category_arr);
            // }
            feature_ranks.add(anova.anovaFValue(category_data));
        }
    	return feature_ranks;
    }

    public double[] process_ssr(DataFrame input) {
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

        SumSquaresRatio ssr = new SumSquaresRatio();
        double[] feature_ranks = ssr.rank(x, y);
        return feature_ranks;
    }

    private final class MutableInt {
        private int value;
        public MutableInt(int value) {
            this.value = value;
        }
        public void increment() { ++value;      }
        public int  get()       { return value; }
    }
}