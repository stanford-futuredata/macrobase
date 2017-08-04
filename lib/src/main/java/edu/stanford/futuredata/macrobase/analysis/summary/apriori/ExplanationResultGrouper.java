package edu.stanford.futuredata.macrobase.analysis.summary.apriori;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExplanationResultGrouper {
    private APExplanation explanation;
    private DataFrame data;

    public ExplanationResultGrouper(
            APExplanation e,
            DataFrame d
    ) {
        this.explanation = e;
        this.data = d;
    }

    public ArrayList<Integer> getMatches(Map<String, String> matcher) {
        int d = matcher.size();
        List<String> colNames = new ArrayList<>(matcher.keySet());

        int n = data.getNumRows();
        ArrayList<Integer> indices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            indices.add(i);
        }
        for (int c = 0; c < d; c++) {
            String colName = colNames.get(c);
            String[] curColumn = data.getStringColumnByName(colName);
            String matchValue = matcher.get(colName);
            ArrayList<Integer> newIndices = new ArrayList<>(indices.size());
            for (int i : indices) {
                if (curColumn[i].equals(matchValue)) {
                    newIndices.add(i);
                }
            }
            indices = newIndices;
        }

        return indices;
    }

    public void group() {
        ArrayList<ExplanationResult> results = explanation.getResults();
        ArrayList<ArrayList<Integer>> resultMatches = new ArrayList<>(results.size());
    }
}
