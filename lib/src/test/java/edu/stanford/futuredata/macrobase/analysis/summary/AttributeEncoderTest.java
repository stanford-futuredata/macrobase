package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AttributeEncoderTest {
    private AttributeEncoder e = new AttributeEncoder();

    @SuppressWarnings("unused")
    private void printItemsets(List<Set<Integer>> results) {
        for (Set<Integer> itemset : results) {
            System.out.println(itemset);
            for (int i : itemset) {
                System.out.println(e.decodeColumn(i) + ":" + e.decodeValue(i));
            }
        }
    }

    @Test
    public void encodeColumns() {
        List<String[]> columns = new ArrayList<>();
        for (int j = 0; j < 2; j ++) {
            String[] curCol = new String[15];
            for (int i = 0; i < 15; i++) {
                curCol[i] = String.valueOf(i % (j * 2 + 3));
            }
            columns.add(curCol);
        }

        List<Set<Integer>> results = e.encodeAttributesAsSets(columns);
        assertEquals(results.size(), columns.get(0).length);

        Set<Integer> totalItems = new HashSet<>();
        for (Set<Integer> itemset : results) {
            totalItems.addAll(itemset);
        }
        assertEquals(totalItems.size(), 5 + 3);
    }
}