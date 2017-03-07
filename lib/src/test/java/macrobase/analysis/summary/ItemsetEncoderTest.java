package macrobase.analysis.summary;

import macrobase.analysis.summary.ItemsetEncoder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class ItemsetEncoderTest {
    @Test
    public void encodeRows() throws Exception {
        List<String[]> rows = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            String[] curRow = new String[2];
            curRow[0] = String.valueOf(i%3);
            curRow[1] = String.valueOf(i%5);
            rows.add(curRow);
        }

        ItemsetEncoder e = new ItemsetEncoder();
        List<Set<Integer>> results = e.encodeRows(rows);
        assertEquals(results.size(), rows.size());

        Set<Integer> totalItems = new HashSet<>();
        for (Set<Integer> itemset : results) {
            totalItems.addAll(itemset);
        }
        assertEquals(totalItems.size(), 5+3);
//
//        for (Set<Integer> itemset : results) {
//            System.out.println(itemset);
//            for (int i : itemset) {
//                System.out.println(e.decodeColumn(i)+":"+e.decodeValue(i));
//            }
//        }
    }
}