package macrobase.analysis.summary.itemset;

import macrobase.analysis.summary.itemset.ItemsetEncoder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class ItemsetEncoderTest {
    @Test
    public void encodeColumns() throws Exception {
        List<String[]> columns = new ArrayList<>();
        for (int j = 0; j < 2; j ++) {
            String[] curCol = new String[15];
            for (int i = 0; i < 15; i++) {
                curCol[i] = String.valueOf(i % (j * 2 + 3));
            }
            columns.add(curCol);
        }

        ItemsetEncoder e = new ItemsetEncoder();
        List<Set<Integer>> results = e.encodeColumns(columns);
        assertEquals(results.size(), columns.get(0).length);

        Set<Integer> totalItems = new HashSet<>();
        for (Set<Integer> itemset : results) {
            totalItems.addAll(itemset);
        }
        assertEquals(totalItems.size(), 5 + 3);
//
//        for (Set<Integer> itemset : results) {
//            System.out.println(itemset);
//            for (int i : itemset) {
//                System.out.println(e.decodeColumn(i) + ":" + e.decodeValue(i));
//            }
//        }
    }
}