package edu.stanford.futuredata.macrobase.analysis.preprocess;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.operator.Transformer;

import edu.stanford.futuredata.macrobase.analysis.stats.MAD;

import org.apache.commons.math3.special.Erf;

import java.lang.Math;
import java.lang.Double;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class Deduplicator {
	private int samplingRate = 1;
	private List<RemovedColumn> removedColumns;

	public class RemovedColumn {
		public String removedColumn;
		public String matchedColumn;
		public double similarity;

		public RemovedColumn(String removedColumn, String matchedColumn, double similarity) {
			this.removedColumn = removedColumn;
			this.matchedColumn = matchedColumn;
			this.similarity = similarity;
		}
	}

	public Deduplicator() {
		removedColumns = new ArrayList<RemovedColumn>();
	}

	public DataFrame deduplicate(DataFrame input) {
		DataFrame other = getRandomSample(input);
		List<String> columnNames = other.getSchema().getColumnNamesByType(Schema.ColType.DOUBLE);
		List<double[]> columns = new ArrayList<double[]>();
		for (int i = 0; i < columnNames.size(); i++) {
			double[] col = other.getDoubleColumnByName(columnNames.get(i));
			columns.add(col);
		}
		List<String> columnsToKeep = new ArrayList<String>();
		removedColumns.clear();
		boolean keep = true;
		for (int i = 0; i < columns.size(); i++) {
			int j = i+1;
			double similarity = 0;
			for (; j < columns.size(); j++) {
				similarity = cosineSimilarity(columns.get(i), columns.get(j));
				System.out.format("Comparing %s and %s, similarity %f\n",
					columnNames.get(i), columnNames.get(j), similarity);
				if (similarity > 0.9) {
					keep = false;
					break;
				}
			}
			if (keep) {
				columnsToKeep.add(columnNames.get(i));
			} else {
				RemovedColumn col = new RemovedColumn(columnNames.get(i), columnNames.get(j),
					similarity);
				removedColumns.add(col);
			}
			keep = true;
		}
		ArrayList<Integer> columnIdxToKeep = other.getSchema().getColumnIndices(columnsToKeep);
		return input.select(columnIdxToKeep);
	}

	private DataFrame getRandomSample(DataFrame input) {
        Integer[] arr = new Integer[input.getNumRows()];
        for (int i = 0; i < input.getNumRows(); i++) {
            arr[i] = i;
        }
        Collections.shuffle(Arrays.asList(arr));

        int sampleSize = input.getNumRows() / samplingRate;
        boolean[] mask = new boolean[input.getNumRows()];
        for (int i = 0; i < sampleSize; i++) {
            mask[arr[i]] = true;
        }

        return input.filter(mask);
    }

    private double cosineSimilarity(double[] vectorA, double[] vectorB) {
	    double dotProduct = 0.0;
	    double normA = 0.0;
	    double normB = 0.0;
	    for (int i = 0; i < vectorA.length; i++) {
	        dotProduct += vectorA[i] * vectorB[i];
	        normA += Math.pow(vectorA[i], 2);
	        normB += Math.pow(vectorB[i], 2);
	    }   
	    return Math.abs(dotProduct) / (Math.sqrt(normA) * Math.sqrt(normB));
	}

    public Deduplicator setSamplingRate(int samplingRate) {
        this.samplingRate = samplingRate;
        return this;
    }

    public List<RemovedColumn> getRemovedColumns() {
    	return removedColumns;
    }
}